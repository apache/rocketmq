/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.transaction;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.StoreUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TRANS_COLUMN_FAMILY;

public class TransMessageRocksDBStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final String REMOVE_TAG = "d";
    private static final byte[] FILL_BYTE = new byte[] {(byte) 0};
    private static final int DEFAULT_CAPACITY = 100000;
    private static final int BATCH_SIZE = 1000;
    private static final int MAX_GET_MSG_TIMES = 3;
    private static final int INITIAL = 0, RUNNING = 1, SHUTDOWN = 2;
    private volatile int state = INITIAL;

    private final MessageStore messageStore;
    private final MessageStoreConfig storeConfig;
    private final MessageRocksDBStorage messageRocksDBStorage;
    private final BrokerStatsManager brokerStatsManager;
    private final SocketAddress storeHost;
    private ThreadLocal<ByteBuffer> bufferLocal = null;
    private TransIndexBuildService transIndexBuildService;
    protected BlockingQueue<TransRocksDBRecord> originTransMsgQueue;

    public TransMessageRocksDBStore(final MessageStore messageStore, final BrokerStatsManager brokerStatsManager, final SocketAddress storeHost) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getMessageStoreConfig();
        this.messageRocksDBStorage = messageStore.getMessageRocksDBStorage();
        this.brokerStatsManager = brokerStatsManager;
        this.storeHost = storeHost;
        bufferLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(storeConfig.getMaxMessageSize()));
        if (storeConfig.isTransRocksDBEnable()) {
            init();
        }
    }

    private void init() {
        if (this.state == RUNNING) {
            return;
        }
        this.transIndexBuildService = new TransIndexBuildService();
        this.originTransMsgQueue = new LinkedBlockingDeque<>(DEFAULT_CAPACITY);
        this.transIndexBuildService.start();
        this.state = RUNNING;
        Long lastOffsetPy = messageRocksDBStorage.getLastOffsetPy(TRANS_COLUMN_FAMILY);
        log.info("TransMessageRocksDBStore start success, lastOffsetPy: {}", lastOffsetPy);
    }

    public void shutdown() {
        if (this.state != RUNNING || this.state == SHUTDOWN) {
            return;
        }
        if (null != this.transIndexBuildService) {
            this.transIndexBuildService.shutdown();
        }
        this.state = SHUTDOWN;
        log.info("TransMessageRocksDBStore shutdown success");
    }

    public void buildTransIndex(DispatchRequest dispatchRequest) {
        if (null == dispatchRequest || dispatchRequest.getCommitLogOffset() < 0L || dispatchRequest.getMsgSize() <= 0 || state != RUNNING || null == this.originTransMsgQueue) {
            logError.error("TransMessageRocksDBStore buildTransIndex error, dispatchRequest: {}, state: {}, originTransMsgQueue: {}", dispatchRequest, state, originTransMsgQueue);
            return;
        }
        long reqOffsetPy = dispatchRequest.getCommitLogOffset();
        long endOffsetPy = messageRocksDBStorage.getLastOffsetPy(TRANS_COLUMN_FAMILY);
        if (reqOffsetPy < endOffsetPy) {
            if (System.currentTimeMillis() % 1000 == 0) {
                log.warn("TransMessageRocksDBStore buildTransIndex recover, but ignore, reqOffsetPy: {}, endOffsetPy: {}", reqOffsetPy, endOffsetPy);
            }
            return;
        }
        int reqMsgSize = dispatchRequest.getMsgSize();
        try {
            MessageExt msgInner = getMessage(reqOffsetPy, reqMsgSize);
            if (null == msgInner) {
                logError.error("TransMessageRocksDBStore buildTransIndex error, msgInner is not found, reqOffsetPy: {}, reqMsgSize: {}", reqOffsetPy, reqMsgSize);
                return;
            }
            String topic = msgInner.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC);
            String uniqKey = msgInner.getUserProperty(MessageConst.PROPERTY_TRANSACTION_ID);
            if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(uniqKey)) {
                logError.error("TransMessageRocksDBStore buildTransIndex error, uniqKey: {}, topic: {}", uniqKey, topic);
                return;
            }
            TransRocksDBRecord transRocksDBRecord = null;
            String reqTopic = dispatchRequest.getTopic();
            if (TopicValidator.RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC.equals(reqTopic)) {
                transRocksDBRecord = new TransRocksDBRecord(reqOffsetPy, topic, uniqKey, reqMsgSize, 0);
            } else if (TopicValidator.RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC.equals(reqTopic)) {
                long offsetPy = -1L;
                String transOffsetPy = null;
                try {
                    transOffsetPy = msgInner.getUserProperty(MessageConst.PROPERTY_TRANS_OFFSET);
                    if (!StringUtils.isEmpty(transOffsetPy)) {
                        offsetPy = Long.parseLong(transOffsetPy);
                    }
                    if (offsetPy >= 0L) {
                        transRocksDBRecord = new TransRocksDBRecord(offsetPy, topic, uniqKey, true);
                    }
                } catch (Exception e) {
                    logError.error("TransMessageRocksDBStore buildTransIndex error, transOffsetPy: {}, error: {}", transOffsetPy, e.getMessage());
                }
            }
            if (null != transRocksDBRecord) {
                while (!originTransMsgQueue.offer(transRocksDBRecord, 3, TimeUnit.SECONDS)) {
                    if (System.currentTimeMillis() % 1000 == 0) {
                        logError.error("TransMessageRocksDBStore buildTransStatus offer transRocksDBRecord error, topic: {}, uniqKey: {}, reqOffsetPy: {}", topic, uniqKey, reqOffsetPy);
                    }
                }
            }
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStore buildTransStatus error: {}", e.getMessage());
        }
    }

    public void deletePrepareMessage(MessageExt messageExt) {
        if (null == messageExt) {
            logError.error("TransMessageRocksDBStore deletePrepareMessage error, messageExt is null");
            return;
        }
        try {
            MessageExtBrokerInner msgInner = makeOpMessageInner(messageExt);
            if (null == msgInner) {
                logError.error("TransMessageRocksDBStore deletePrepareMessage msgInner is null");
                return;
            }
            PutMessageResult result = messageStore.putMessage(msgInner);
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                this.brokerStatsManager.incTopicPutNums(msgInner.getTopic());
                this.brokerStatsManager.incTopicPutSize(msgInner.getTopic(), result.getAppendMessageResult().getWroteBytes());
                this.brokerStatsManager.incBrokerPutNums();
                return;
            }
            logError.error("TransMessageRocksDBStore deletePrepareMessage put op msg failed, result: {}", result);
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStore deletePrepareMessage error: {}", e.getMessage());
        }
    }

    public MessageExt getMessage(long offsetPy, int sizePy) {
        if (offsetPy < 0L || sizePy <= 0 || sizePy > storeConfig.getMaxMessageSize()) {
            logError.error("TransMessageRocksDBStore getMessage param error, offsetPy: {}, sizePy: {}, maxMsgSizeConfig: {}", offsetPy, sizePy, storeConfig.getMaxMessageSize());
            return null;
        }
        ByteBuffer byteBuffer = bufferLocal.get();
        if (sizePy > byteBuffer.limit()) {
            bufferLocal.remove();
            byteBuffer = ByteBuffer.allocate(sizePy);
            bufferLocal.set(byteBuffer);
        }
        for (int i = 0; i < MAX_GET_MSG_TIMES; i++) {
            try {
                MessageExt msgExt = StoreUtil.getMessage(offsetPy, sizePy, messageStore, bufferLocal.get());
                if (null == msgExt) {
                    log.warn("Fail to read msg from commitLog offsetPy:{} sizePy:{}", offsetPy, sizePy);
                } else {
                    return msgExt;
                }
            } catch (Exception e) {
                logError.error("TransMessageRocksDBStore getMessage error, offsetPy: {}, sizePy: {}, error: {}", offsetPy, sizePy, e.getMessage());
            }
        }
        return null;
    }

    public MessageRocksDBStorage getMessageRocksDBStorage() {
        return messageRocksDBStorage;
    }

    private MessageExtBrokerInner makeOpMessageInner(MessageExt messageExt) {
        if (null == messageExt) {
            logError.error("TransMessageRocksDBStore makeOpMessageInner messageExt is null");
            return null;
        }
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(TopicValidator.RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC);
            msgInner.setBody(FILL_BYTE);
            msgInner.setQueueId(0);
            msgInner.setTags(REMOVE_TAG);
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(this.storeHost);
            msgInner.setStoreHost(this.storeHost);
            msgInner.setWaitStoreMsgOK(false);
            MessageClientIDSetter.setUniqID(msgInner);
            String uniqKey = MessageClientIDSetter.getUniqID(messageExt);
            if (!StringUtils.isEmpty(uniqKey)) {
                MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_ID, uniqKey);
            }
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANS_OFFSET, String.valueOf(messageExt.getCommitLogOffset()));
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, messageExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            return msgInner;
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStore makeOpMessageInner error: {}", e.getMessage());
            return null;
        }
    }

    public Integer getCheckTimes(String topic, String uniqKey, Long offsetPy) {
        if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(uniqKey) || null == offsetPy || offsetPy < 0L) {
            return null;
        }
        try {
            TransRocksDBRecord record = messageRocksDBStorage.getRecordForTrans(TRANS_COLUMN_FAMILY, new TransRocksDBRecord(offsetPy, topic, uniqKey, false));
            if (null == record) {
                return null;
            }
            return record.getCheckTimes();
        } catch (Exception e) {
            logError.error("TransMessageRocksDBStore getCheckTimes error, topic: {}, uniqKey: {}, offsetPy: {}, error: {}", topic, uniqKey, offsetPy, e.getMessage());
            return null;
        }
    }

    public boolean isMappedFileMatchedRecover(long phyOffset) {
        if (!storeConfig.isTransRocksDBEnable()) {
            return true;
        }
        Long lastOffsetPy = messageRocksDBStorage.getLastOffsetPy(TRANS_COLUMN_FAMILY);
        log.info("trans isMappedFileMatchedRecover lastOffsetPy: {}", lastOffsetPy);
        if (null != lastOffsetPy && phyOffset <= lastOffsetPy) {
            log.info("isMappedFileMatchedRecover TransMessageRocksDBStore recover form this offset, phyOffset: {}, lastOffsetPy: {}", phyOffset, lastOffsetPy);
            return true;
        }
        return false;
    }

    private String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TransMessageRocksDBStore.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) TransMessageRocksDBStore.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    public class TransIndexBuildService extends ServiceThread {
        private final Logger log = TransMessageRocksDBStore.log;
        private List<TransRocksDBRecord> trs;
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + "service start");
            trs = new ArrayList<>(BATCH_SIZE);
            while (!this.isStopped() || !originTransMsgQueue.isEmpty()) {
                try {
                    buildTransIndex();
                } catch (Exception e) {
                    trs.clear();
                    logError.error("TransMessageRocksDBStore error occurred in: {}, error: {}", getServiceName(), e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        protected void buildTransIndex() {
            pollTransMessageRecords();
            if (CollectionUtils.isEmpty(trs)) {
                return;
            }
            try {
                messageRocksDBStorage.writeRecordsForTrans(TRANS_COLUMN_FAMILY, trs);
            } catch (Exception e) {
                logError.error("TransMessageRocksDBStore pollAndPutTransRequest writeRecords error: {}", e.getMessage());
            }
            trs.clear();
        }

        protected void pollTransMessageRecords() {
            try {
                TransRocksDBRecord firstReq = originTransMsgQueue.poll(100, TimeUnit.MILLISECONDS);
                if (null != firstReq) {
                    trs.add(firstReq);
                    while (true) {
                        TransRocksDBRecord tmpReq = originTransMsgQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (null == tmpReq) {
                            break;
                        }
                        trs.add(tmpReq);
                        if (trs.size() >= BATCH_SIZE) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logError.error("TransMessageRocksDBStore fetchTransMessageRecord error: {}", e.getMessage());
            }
        }
    }
}
