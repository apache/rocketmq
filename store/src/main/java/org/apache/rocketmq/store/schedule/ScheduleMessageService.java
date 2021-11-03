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
package org.apache.rocketmq.store.schedule;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledExecutorService deliverExecutorService;
    private ScheduledExecutorService handleExecutorService;
    private int deliverThreadPoolNums = Runtime.getRuntime().availableProcessors();
    private MessageStore writeMessageStore;
    private int maxDelayLevel;
    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
        new ConcurrentHashMap<>(32);

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            super.load();
            this.deliverExecutorService = new ScheduledThreadPoolExecutor(deliverThreadPoolNums, new DefaultThreadFactory("ScheduleMessageExecutorDeliverThread_"));
            if (this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleAsyncReput()) {
                this.handleExecutorService = new ScheduledThreadPoolExecutor(deliverThreadPoolNums, new DefaultThreadFactory("ScheduleMessageExecutorHandleThread_"));
            }

            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    if (this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleAsyncReput()) {
                        this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    }
                }
            }

            this.deliverExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
            this.deliverExecutorService.shutdown();
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        result = result && this.correctDelayOffset();
        return result;
    }

    public boolean correctDelayOffset() {
        try {
            for (int delayLevel : delayLevelTable.keySet()) {
                ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                long cqMinOffset = cq.getMinOffsetInQueue();
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }

                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
                this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    class DeliverDelayedMessageTimerTask implements Runnable {
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD, TimeUnit.MILLISECONDS);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeup() {
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        boolean enableAsyncReput = this.isEnableScheduleAsyncReput();
                        long currentOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            currentOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;

                            if (countdown <= 0) {
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                msgInner.getTopic(), msgInner);
                                            continue;
                                        }

                                        if (enableAsyncReput) {
                                            // Asynchronous send flow control.
                                            int maxPendingLimit = ScheduleMessageService.this.defaultMessageStore
                                                .getMessageStoreConfig().getScheduleAsyncReputMaxPendingLimit();

                                            int currentPendingNum = this.getDeliverPendingQueue().size();
                                            if (currentPendingNum > maxPendingLimit) {
                                                log.warn("Asynchronous sending triggers flow control, " +
                                                    "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
                                                this.toNextTask(currentOffset);
                                                return;
                                            }

                                            // Handling abnormal blocking.
                                            PutResultProcess putResultProcess = this.getDeliverPendingQueue().peek();
                                            if (putResultProcess != null && putResultProcess.need2Wait()) {
                                                this.toNextTask(currentOffset);
                                                return;
                                            }
                                        }

                                        CompletableFuture<PutMessageResult> future =
                                            ScheduleMessageService.this.writeMessageStore.asyncPutMessage(msgInner);
                                        PutResultProcess putResultProcess = new PutResultProcess()
                                            .setTopic(msgInner.getTopic())
                                            .setDelayLevel(this.delayLevel)
                                            .setOffset(currentOffset)
                                            .setPhysicOffset(offsetPy)
                                            .setPhysicSize(sizePy)
                                            .setMsgId(msgExt.getMsgId())
                                            .setAutoResend(enableAsyncReput)
                                            .setFuture(future)
                                            .thenProcess();

                                        if (enableAsyncReput) {
                                            this.asyncSend(putResultProcess);
                                        } else {
                                            boolean isSuccess = this.syncSend(putResultProcess);
                                            if (isSuccess) {
                                                ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                    putResultProcess.getNextOffset());
                                            } else {
                                                this.toNextTask(currentOffset);
                                                return;
                                            }
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me
                                         */
                                        log.error("ScheduleMessageService, messageTimeup execute error, drop it. msgExt={}, nextOffset={}, offsetPy={}, sizePy={}", msgExt, currentOffset, offsetPy, sizePy, e);
                                    }
                                }
                            } else {
                                this.toNextTask(currentOffset);
                                return;
                            }
                        } // end of for

                        long nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        this.toNextTask(nextOffset);
                        return;
                    } finally {
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    long cqMaxOffset = cq.getMaxOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                            offset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                    }

                    if (offset > cqMaxOffset) {
                        failScheduleOffset = cqMaxOffset;
                        log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                            offset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            this.toNextTask(failScheduleOffset);
        }

        public void toNextTask(long offset) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                this.delayLevel, offset), DELAY_FOR_A_WHILE, TimeUnit.MILLISECONDS);
        }

        public boolean isEnableScheduleAsyncReput() {
            return ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleAsyncReput();
        }

        public LinkedBlockingQueue<PutResultProcess> getDeliverPendingQueue() {
            return ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);
        }

        public void asyncSend(PutResultProcess putResultProcess) {
            this.getDeliverPendingQueue().add(putResultProcess);
        }

        public boolean syncSend(PutResultProcess putResultProcess) {
            putResultProcess.get();
            return putResultProcess.isSuccess();
        }
    }

    class HandlePutResultTask extends TimerTask {
        private final int delayLevel;

        public HandlePutResultTask(int delayLevel) {
            this.delayLevel = delayLevel;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<PutResultProcess> pendingQueue =
                ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            PutResultProcess putResultProcess;
            while ((putResultProcess = pendingQueue.peek()) != null) {
                try {
                    if (putResultProcess.isSuccess()) {
                        ScheduleMessageService.this.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                        pendingQueue.remove();
                        continue;
                    } else if (putResultProcess.isCancelled()) {
                        putResultProcess.onException();
                    }
                } catch (Exception e) {
                    log.error("ScanPutResultTask error", e);
                    putResultProcess.onException();
                }

                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    log.error("ScanPutResultTask sleep error.", e);
                }
            }

            ScheduleMessageService.this.deliverExecutorService
                .schedule(new HandlePutResultTask(this.delayLevel), DELAY_FOR_A_WHILE, TimeUnit.MILLISECONDS);
        }
    }

    private class PutResultProcess {
        private String topic;
        private long offset;
        private long physicOffset;
        private int physicSize;
        private int delayLevel;
        private String msgId;
        private boolean autoResend = false;
        private CompletableFuture<PutMessageResult> future;

        private int resendCount = 0;
        private volatile boolean processSuccess = false;
        private volatile boolean processStop = false;

        public PutResultProcess setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PutResultProcess setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public PutResultProcess setPhysicOffset(long physicOffset) {
            this.physicOffset = physicOffset;
            return this;
        }

        public PutResultProcess setPhysicSize(int physicSize) {
            this.physicSize = physicSize;
            return this;
        }

        public PutResultProcess setDelayLevel(int delayLevel) {
            this.delayLevel = delayLevel;
            return this;
        }

        public PutResultProcess setMsgId(String msgId) {
            this.msgId = msgId;
            return this;
        }

        public PutResultProcess setAutoResend(boolean autoResend) {
            this.autoResend = autoResend;
            return this;
        }

        public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
            this.future = future;
            return this;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextOffset() {
            return offset + 1;
        }

        public long getPhysicOffset() {
            return physicOffset;
        }

        public int getPhysicSize() {
            return physicSize;
        }

        public Integer getDelayLevel() {
            return delayLevel;
        }

        public String getMsgId() {
            return msgId;
        }

        public boolean isAutoResend() {
            return autoResend;
        }

        public CompletableFuture<PutMessageResult> getFuture() {
            return future;
        }

        public int getResendCount() {
            return resendCount;
        }

        public PutResultProcess thenProcess() {
            future.thenAccept(result -> {
                this.handleResult(result);
            });

            future.exceptionally(e -> {
                onException();
                return null;
            });

            return this;
        }

        private void handleResult(PutMessageResult result) {
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                onSuccess(result);
            } else {
                onException();
            }
        }

        public void onSuccess(PutMessageResult result) {
            this.processSuccess = true;
            if (ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleMessageStats()) {
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incBrokerPutNums(result.getAppendMessageResult().getMsgNum());
            }
        }

        public void onException() {
            log.error("ScheduleMessageService deliver failure, info: {}", this.toString());
            if (this.autoResend) {
                this.resend();
            }
        }

        public boolean isSuccess() {
            return this.processSuccess;
        }

        public boolean isCancelled() {
            return this.future.isCancelled();
        }

        public PutMessageResult get() {
            try {
                return future.get();
            } catch (InterruptedException | ExecutionException e) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
            }
        }

        public void shutdown() {
            this.processStop = true;
        }

        private void resend() {
            if (this.processStop) {
                return;
            }

            // Gradually increase the resend interval.
            try {
                Thread.sleep(((resendCount / 3) * 100) % 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(this.physicOffset, this.physicSize);
                if (msgExt == null) {
                    return;
                }

                MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                this.future = ScheduleMessageService.this.writeMessageStore.asyncPutMessage(msgInner);
                this.thenProcess();
                resendCount++;
                log.warn("Resend message, info: {}", this.toString());
            } catch (Exception e) {
                this.future.cancel(true);
                log.error("Resend message error, info: {}", this.toString(), e);
            }
        }

        public boolean need2Wait() {
            return this.resendCount > 18;
        }

        @Override
        public String toString() {
            return "PutResultProcess{" +
                "topic='" + topic + '\'' +
                ", offset=" + offset +
                ", physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", delayLevel=" + delayLevel +
                ", msgId='" + msgId + '\'' +
                ", autoResend=" + autoResend +
                ", resendCount=" + resendCount +
                ", processSuccess=" + processSuccess +
                ", processStop=" + processStop +
                '}';
        }
    }
}
