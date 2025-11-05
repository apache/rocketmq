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
package org.apache.rocketmq.broker.transaction.rocksdb;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import io.netty.channel.Channel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage;
import org.apache.rocketmq.store.transaction.TransRocksDBRecord;
import org.apache.rocketmq.store.transaction.TransMessageRocksDBStore;
import static org.apache.rocketmq.store.rocksdb.MessageRocksDBStorage.TRANS_COLUMN_FAMILY;

public class TransactionalMessageRocksDBService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private static final int MAX_BATCH_SIZE_FROM_ROCKSDB = 2000;
    private static final int INITIAL = 0, RUNNING = 1, SHUTDOWN = 2;
    private volatile int state = INITIAL;

    private final MessageRocksDBStorage messageRocksDBStorage;
    private final TransMessageRocksDBStore transMessageRocksDBStore;
    private final MessageStore messageStore;
    private final BrokerController brokerController;

    private TransStatusCheckService transStatusService;
    private ExecutorService checkTranStatusTaskExecutor;

    public TransactionalMessageRocksDBService(final MessageStore messageStore, final BrokerController brokerController) {
        this.messageStore = messageStore;
        this.transMessageRocksDBStore = messageStore.getTransRocksDBStore();
        this.messageRocksDBStorage = transMessageRocksDBStore.getMessageRocksDBStorage();
        this.brokerController = brokerController;
    }

    public void start() {
        if (this.state == RUNNING) {
            return;
        }
        initService();
        this.transStatusService.start();
        this.state = RUNNING;
        log.info("TransactionalMessageRocksDBService start success");
    }

    private void initService() {
        this.transStatusService = new TransStatusCheckService();
        this.checkTranStatusTaskExecutor = ThreadUtils.newThreadPoolExecutor(
            2,
            5,
            100,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(2000),
            new ThreadFactoryImpl("Transaction-rocksdb-msg-check-thread", brokerController.getBrokerIdentity()),
            new CallerRunsPolicy());
    }

    public void shutdown() {
        if (this.state != RUNNING || this.state == SHUTDOWN) {
            return;
        }
        if (null != this.transStatusService) {
            this.transStatusService.shutdown();
        }
        if (null != this.checkTranStatusTaskExecutor) {
            this.checkTranStatusTaskExecutor.shutdown();
        }
        this.state = SHUTDOWN;
        log.info("TransactionalMessageRocksDBService shutdown success");
    }

    private void checkTransStatus() {
        long count = 0;
        byte[] lastKey = null;
        while (true) {
            try {
                List<TransRocksDBRecord> trs = messageRocksDBStorage.scanRecordsForTrans(TRANS_COLUMN_FAMILY, MAX_BATCH_SIZE_FROM_ROCKSDB, lastKey);
                if (CollectionUtils.isEmpty(trs)) {
                    log.info("TransactionalMessageRocksDBService checkTransStatus trs is empty");
                    break;
                }
                count += trs.size();
                checkTransRecordsStatus(trs);
                lastKey = trs.size() >= MAX_BATCH_SIZE_FROM_ROCKSDB ? trs.get(trs.size() - 1).getKeyBytes() : null;
                if (null == lastKey) {
                    break;
                }
            } catch (Exception e) {
                log.error("TransactionalMessageRocksDBService checkTransStatus error, error: {}, count: {}", e.getMessage(), count);
                break;
            }
        }
        log.info("TransactionalMessageRocksDBService checkTransStatus count: {}", count);
    }

    private void checkTransRecordsStatus(List<TransRocksDBRecord> trs) {
        if (CollectionUtils.isEmpty(trs)) {
            log.error("TransactionalMessageRocksDBService checkTransRecordsStatus, trs is empty");
            return;
        }
        try {
            List<TransRocksDBRecord> updateList = new ArrayList<>();
            for (TransRocksDBRecord halfRecord : trs) {
                if (null == halfRecord) {
                    log.error("TransactionalMessageRocksDBService checkTransRecordsStatus, halfRecord is null");
                    continue;
                }
                try {
                    if (halfRecord.getCheckTimes() > brokerController.getBrokerConfig().getTransactionCheckMax()) {
                        halfRecord.setDelete(true);
                        updateList.add(halfRecord);
                        log.info("TransactionalMessageRocksDBService checkTransRecordsStatus checkTimes > {}, need delete, checkTimes: {}, msgId: {}", brokerController.getBrokerConfig().getTransactionCheckMax(), halfRecord.getCheckTimes(), halfRecord.getUniqKey());
                        continue;
                    }
                    MessageExt msgExt = transMessageRocksDBStore.getMessage(halfRecord.getOffsetPy(), halfRecord.getSizePy());
                    if (null == msgExt) {
                        log.error("TransactionalMessageRocksDBService checkTransRecordsStatus, msgExt is null, offsetPy: {}, sizePy: {}", halfRecord.getOffsetPy(), halfRecord.getSizePy());
                        halfRecord.setDelete(true);
                        updateList.add(halfRecord);
                        continue;
                    }
                    if (!isImmunityTimeExpired(msgExt)) {
                        continue;
                    }
                    resolveHalfMsg(msgExt);
                    halfRecord.setCheckTimes(halfRecord.getCheckTimes() + 1);
                    if (halfRecord.getCheckTimes() > brokerController.getBrokerConfig().getTransactionCheckMax()) {
                        halfRecord.setDelete(true);
                        log.info("TransactionalMessageRocksDBService checkTransRecordsStatus checkTimes > {}, need delete, checkTimes: {}, msgId: {}", brokerController.getBrokerConfig().getTransactionCheckMax(), halfRecord.getCheckTimes(), halfRecord.getUniqKey());
                    }
                    updateList.add(halfRecord);
                } catch (Exception e) {
                    log.error("TransactionalMessageRocksDBService checkTransRecordsStatus error : {}", e.getMessage());
                }
            }
            if (!CollectionUtils.isEmpty(updateList)) {
                messageRocksDBStorage.updateRecordsForTrans(TRANS_COLUMN_FAMILY, updateList);
            }
        } catch (Exception e) {
            log.error("TransactionalMessageRocksDBService checkTransRecordsStatus error: {}", e.getMessage());
        }
    }

    private boolean isImmunityTimeExpired(MessageExt msgExt) {
        String immunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
        long immunityTime = brokerController.getBrokerConfig().getTransactionTimeOut();
        if (!StringUtils.isEmpty(immunityTimeStr)) {
            try {
                immunityTime = Long.parseLong(immunityTimeStr);
                immunityTime *= 1000;
            } catch (Exception e) {
                log.error("parse immunityTimesStr error: {}, msgId: {}", e.getMessage(), msgExt.getMsgId());
            }
        }
        if ((System.currentTimeMillis() - msgExt.getBornTimestamp()) < immunityTime) {
            return false;
        }
        return true;
    }

    private String getServiceThreadName() {
        String brokerIdentifier = "";
        if (TransactionalMessageRocksDBService.this.messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore messageStore = (DefaultMessageStore) TransactionalMessageRocksDBService.this.messageStore;
            if (messageStore.getBrokerConfig().isInBrokerContainer()) {
                brokerIdentifier = messageStore.getBrokerConfig().getIdentifier();
            }
        }
        return brokerIdentifier;
    }

    private void resolveHalfMsg(final MessageExt msgExt) {
        if (checkTranStatusTaskExecutor != null) {
            checkTranStatusTaskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendCheckMessage(msgExt);
                    } catch (Exception e) {
                        log.error("TransactionalMessageRocksDBService Send check message error: {}, msgId: {}", e.getMessage(), msgExt.getMsgId());
                    }
                }
            });
        } else {
            log.error("TransactionalMessageRocksDBService checkTranStatusTaskExecutor not init, msgId: {}", msgExt.getMsgId());
        }
    }

    private void sendCheckMessage(MessageExt msgExt) {
        if (null == msgExt) {
            log.info("TransactionalMessageRocksDBService sendCheckMessage msgExt is null");
            return;
        }
        try {
            CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
            checkTransactionStateRequestHeader.setTopic(msgExt.getTopic());
            checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
            checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
            checkTransactionStateRequestHeader.setMsgId(MessageClientIDSetter.getUniqID(msgExt));
            checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
            checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());
            checkTransactionStateRequestHeader.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
            msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
            msgExt.setStoreSize(0);
            String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            Channel channel = brokerController.getProducerManager().getAvailableChannel(groupId);
            if (channel != null) {
                brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
            } else {
                log.warn("TransactionalMessageRocksDBService checkProducerTransactionState failed, channel is null. groupId: {}, msgId: {}", groupId, msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.error("TransactionalMessageRocksDBService sendCheckMessage error: {}, msgId: {}", e.getMessage(), msgExt.getMsgId());
        }
    }

    private class TransStatusCheckService extends ServiceThread {
        private final Logger log = TransactionalMessageRocksDBService.log;
        @Override
        public String getServiceName() {
            return getServiceThreadName() + this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service start");
            while (!this.isStopped()) {
                try {
                    long begin = System.currentTimeMillis();
                    checkTransStatus();
                    log.info("TransactionalMessageRocksDBService ScanTransAndStatusCheckService check trans status, check cost: {}", System.currentTimeMillis() - begin);
                    waitForRunning(brokerController.getBrokerConfig().getTransactionCheckInterval());
                } catch (Exception e) {
                    log.error("TransactionalMessageRocksDBService ScanTransAndStatusCheckService error: {}", e.getMessage());
                }
            }
            log.info(this.getServiceName() + " service end");
        }
    }
}
