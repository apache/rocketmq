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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class NonPersistentMessageStore{
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;

    //talbe根据topic获取consumeQueue
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, NonPersistentConsumeQueue>> nonPersistentConsumeQueueTable;

    private final NonPersistentConsumeQueue msgQueueLog;

    private final StoreStatsService storeStatsService;

    private final TransientStorePool transientStorePool;

    private final RunningFlags runningFlags = new RunningFlags();

    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    private AtomicLong printTimes = new AtomicLong(0);

    private final LinkedList<CommitLogDispatcher> dispatcherList;

    PutMessageLock putMessageLock;


    public NonPersistentMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                                     final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;

        //
        this.nonPersistentConsumeQueueTable = new ConcurrentHashMap<>(32);
        this.msgQueueLog = new NonPersistentConsumeQueue(this);

        this.storeStatsService = new StoreStatsService();

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.dispatcherList = new LinkedList<>();
    }

    /**
     * @throws Exception
     */
    public void start() throws Exception {

        this.storeStatsService.start();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();

            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            this.storeStatsService.shutdown();
        }

        this.transientStorePool.destroy();

    }

    /**
     * 非持久化putMessage
     * 在JVM中通过CopyOnWriteArrayList实现消息的同步非持久化存储，将消息存入到对应Topic、queueId的两层CurrentMap中
     */
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        PutMessageResult putMessageResult;
        AppendMessageResult result = null;
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            String topic = msg.getTopic();
            int queueId = msg.getQueueId();
            String propertiesString = msg.getPropertiesString();
            NonPersistentMsg msg1 = new NonPersistentMsg();
            msg1.setTopic(topic);
            msg1.setQueueId(queueId);
            msg1.setMessageProperties(propertiesString);

            ConcurrentMap<Integer/* queueId */, NonPersistentConsumeQueue> map = this.nonPersistentConsumeQueueTable.get(topic);
            if (null == map){
                map = new ConcurrentHashMap<Integer, NonPersistentConsumeQueue>();
            }
            msgQueueLog.getMsgCopyOnWriteArrayList().addIfAbsent(msg1);
            map.putIfAbsent(queueId, msgQueueLog);
            nonPersistentConsumeQueueTable.putIfAbsent(topic, map);
        } finally {
            putMessageLock.unlock();
        }
        putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        if (!putMessageResult.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return putMessageResult;
    }

    /**
     * 非持久化getMessage，从CopyOnWriteArrayList根据offset获取消息
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums,
        final MessageFilter messageFilter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        //获取物理最大偏移量 TODO
        final long maxOffsetPy = this.msgQueueLog.getMaxOffset();

        //队列
        NonPersistentConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
            } else {
                //根据偏移量获取队列

                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getMessageResult(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;

                            if (messageFilter != null
                                && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.msgQueueLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                continue;
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }


                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                            * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                        + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }
    /**
     * 根据topic，queueID获取消息队列，这个消息队列是个CopyOnWriteArrayList实现
     */
    public NonPersistentConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, NonPersistentConsumeQueue> map = nonPersistentConsumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, NonPersistentConsumeQueue> newMap = new ConcurrentHashMap<Integer, NonPersistentConsumeQueue>(128);
            ConcurrentMap<Integer, NonPersistentConsumeQueue> oldMap = nonPersistentConsumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        NonPersistentConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            NonPersistentConsumeQueue newLogic = new NonPersistentConsumeQueue(
                    topic,
                    queueId,
                    this);
            NonPersistentConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        NonPersistentConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        NonPersistentConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        NonPersistentConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getMessageResult(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.msgQueueLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.msgQueueLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.msgQueueLog.getMessageResult(offset);
    }
}
