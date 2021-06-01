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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerPeriodicConcurrently;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.StageOffsetStore;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.concurrent.PriorityConcurrentEngine;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessagePeriodicConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerPeriodicConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor dispatchExecutor;
    private final ThreadPoolExecutor consumeExecutor;
    private final PriorityConcurrentEngine engine;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;
    private final List<Integer> summedStageDefinitions;
    private final ConcurrentMap<String/*topic*/, AtomicInteger/*currentStageOffset*/> currentStageOffsetMap = new ConcurrentHashMap<>();
    private final int pullBatchSize;
    private final StageOffsetStore stageOffsetStore;

    public ConsumeMessagePeriodicConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerPeriodicConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;
        this.summedStageDefinitions = new ArrayList<>();
        Collection<Integer> definitions = messageListener.getStageDefinitions();
        if (definitions != null) {
            int sum = 0;
            for (Integer stageDefinition : definitions) {
                this.summedStageDefinitions.add(sum = sum + stageDefinition);
            }
        }
        this.stageOffsetStore = this.defaultMQPushConsumerImpl.getStageOffsetStore();

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.pullBatchSize = this.defaultMQPushConsumer.getPullBatchSize();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        int consumeThreadMin = this.defaultMQPushConsumer.getConsumeThreadMin();
        int consumeThreadMax = this.defaultMQPushConsumer.getConsumeThreadMax();
        this.dispatchExecutor = new ThreadPoolExecutor(
            (int) Math.ceil(consumeThreadMin * 1.0 / this.pullBatchSize),
            (int) Math.ceil(consumeThreadMax * 1.0 / this.pullBatchSize),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryImpl("DispatchMessageThread_"));
        // when the number of threads is equal to
        // the topic consumeQueue size multiplied by this.pullBatchSize,
        // good performance can be obtained
        this.consumeExecutor = new ThreadPoolExecutor(
            consumeThreadMin,
            consumeThreadMax,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));
        engine = new PriorityConcurrentEngine(this.consumeExecutor);

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    @Override
    public void start() {
        engine.start();
        if (MessageModel.CLUSTERING.equals(ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessagePeriodicConcurrentlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.dispatchExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        engine.shutdown(awaitTerminateMillis);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    public AtomicInteger getCurrentStageOffset(MessageQueue messageQueue, String topic) {
        AtomicInteger index = currentStageOffsetMap.get(topic);
        if (null == index) {
            int stageOffset = stageOffsetStore == null ? 0 : stageOffsetStore.readStageOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
            if (stageOffset < 0) {
                stageOffset = 0;
            }
            currentStageOffsetMap.putIfAbsent(topic, new AtomicInteger(stageOffset));
            index = currentStageOffsetMap.get(topic);
        }
        return index;
    }

    public int getCurrentLeftoverStage(MessageQueue messageQueue, String topic) {
        for (Integer stageDefinition : summedStageDefinitions) {
            int left = stageDefinition - getCurrentStageOffset(messageQueue, topic).get();
            if (left > 0) {
                return left;
            }
        }
        return -1;
    }

    public int getCurrentLeftoverStageIndex(MessageQueue messageQueue, String topic) {
        for (int i = 0; i < summedStageDefinitions.size(); i++) {
            int left = summedStageDefinitions.get(i) - getCurrentStageOffset(messageQueue, topic).get();
            if (left > 0) {
                return i;
            }
        }
        return -1;
    }

    public int getCurrentLeftoverStageIndexAndUpdate(MessageQueue messageQueue, String topic) {
        return getCurrentLeftoverStageIndexAndUpdate(messageQueue, topic, 1);
    }

    public int getCurrentLeftoverStageIndexAndUpdate(MessageQueue messageQueue, String topic, int delta) {
        try {
            return getCurrentLeftoverStageIndex(messageQueue, topic);
        } finally {
            final AtomicInteger index = getCurrentStageOffset(messageQueue, topic);
            synchronized (index) {
                index.getAndAdd(delta);
            }
        }
    }

    public int increaseCurrentStage(MessageQueue messageQueue, String topic) {
        return increaseCurrentStage(messageQueue, topic, 1);
    }

    public int increaseCurrentStage(MessageQueue messageQueue, String topic, int delta) {
        final AtomicInteger index = getCurrentStageOffset(messageQueue, topic);
        synchronized (index) {
            return index.getAndAdd(delta);
        }
    }

    public int decrementCurrentStage(MessageQueue messageQueue, String topic) {
        return decrementCurrentStage(messageQueue, topic, 1);
    }

    public int decrementCurrentStage(MessageQueue messageQueue, String topic, int delta) {
        final AtomicInteger index = getCurrentStageOffset(messageQueue, topic);
        synchronized (index) {
            return index.getAndSet(index.get() - delta);
        }
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        String topic = msg.getTopic();
        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(topic);
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        Set<MessageQueue> topicSubscribeInfo = this.defaultMQPushConsumerImpl.getRebalanceImpl().getTopicSubscribeInfo(topic);
        MessageQueue messageQueue = null;
        if (CollectionUtils.isNotEmpty(topicSubscribeInfo)) {
            for (MessageQueue queue : topicSubscribeInfo) {
                if (queue.getQueueId() == msg.getQueueId()) {
                    messageQueue = queue;
                    break;
                }
            }
        }

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context, this.getCurrentLeftoverStageIndexAndUpdate(messageQueue, topic, msgs.size()));
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        decrementCurrentStage(messageQueue, topic, msgs.size());
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessagePeriodicConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }
        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {
        if (System.currentTimeMillis() < ConsumeMessagePeriodicConcurrentlyService.this.messageListener.getConsumeFromTimeStamp()) {
            return;
        }
        if (dispatchToConsume) {
            DispatchRequest dispatchRequest = new DispatchRequest(processQueue, messageQueue);
            this.dispatchExecutor.submit(dispatchRequest);
        }
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
        final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessagePeriodicConcurrentlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessagePeriodicConcurrentlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessagePeriodicConcurrentlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessagePeriodicConcurrentlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        MessageQueue messageQueue = consumeRequest.getMessageQueue();
        String topic = messageQueue.getTopic();
        boolean continueConsume = true;
        long commitOffset = -1L;
        int commitStageOffset = -1;
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                        messageQueue);
                case SUCCESS:
                    commitOffset = consumeRequest.getProcessQueue().commitMessages(msgs);
                    commitStageOffset = getCurrentStageOffset(messageQueue, topic).get();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    decrementCurrentStage(messageQueue, topic, msgs.size());
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            messageQueue,
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commitMessages(msgs);
                        commitStageOffset = getCurrentStageOffset(messageQueue, topic).get();
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commitMessages(msgs);
                    commitStageOffset = getCurrentStageOffset(messageQueue, topic).get();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        messageQueue,
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    decrementCurrentStage(messageQueue, topic, msgs.size());
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            messageQueue,
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(messageQueue, commitOffset, false);
        }
        if (stageOffsetStore != null && commitStageOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            stageOffsetStore.updateStageOffset(messageQueue, commitStageOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                } else {
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    class DispatchRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public DispatchRequest(ProcessQueue processQueue,
            MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            String topic = this.messageQueue.getTopic();
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                if (MessageModel.BROADCASTING.equals(ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (final AtomicBoolean continueConsume = new AtomicBoolean(true); continueConsume.get(); ) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessagePeriodicConcurrentlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessagePeriodicConcurrentlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessagePeriodicConcurrentlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize =
                            ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
                        int currentLeftoverStage = ConsumeMessagePeriodicConcurrentlyService.this.getCurrentLeftoverStage(this.messageQueue, topic);
                        int takeSize = ConsumeMessagePeriodicConcurrentlyService.this.pullBatchSize * consumeBatchSize;
                        if (0 < currentLeftoverStage && currentLeftoverStage < takeSize) {
                            takeSize = currentLeftoverStage;
                        }

                        List<MessageExt> msgs = this.processQueue.takeMessages(takeSize);
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
                        if (!msgs.isEmpty()) {
                            List<List<MessageExt>> lists = UtilAll.partition(msgs, consumeBatchSize);
                            for (final List<MessageExt> list : lists) {
                                int currentLeftoverStageIndex =
                                    ConsumeMessagePeriodicConcurrentlyService.this.getCurrentLeftoverStageIndexAndUpdate(this.messageQueue, topic, list.size());
                                ConsumeRequest consumeRequest = new ConsumeRequest(list, this.processQueue, this.messageQueue,
                                    continueConsume, currentLeftoverStageIndex);
                                if (currentLeftoverStageIndex >= 0) {
                                    engine.runPriorityAsync(currentLeftoverStageIndex, consumeRequest);
                                } else {
                                    engine.runPriorityAsync(consumeRequest);
                                }
                            }
                        } else {
                            continueConsume.set(false);
                        }
                    }
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessagePeriodicConcurrentlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;
        private final AtomicBoolean continueConsume;
        private final int currentLeftoverStageIndex;

        public ConsumeRequest(List<MessageExt> msgs,
            ProcessQueue processQueue,
            MessageQueue messageQueue,
            AtomicBoolean continueConsume,
            int currentLeftoverStage) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
            this.continueConsume = continueConsume;
            this.currentLeftoverStageIndex = currentLeftoverStage;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            String topic = this.messageQueue.getTopic();
            ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
            ConsumeOrderlyStatus status = null;

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext
                    .setConsumerGroup(ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                // init the consume context type
                consumeMessageContext.setProps(new HashMap<String, String>());
                ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            boolean hasException = false;
            try {
                this.processQueue.getConsumeLock().lock();
                if (this.processQueue.isDropped()) {
                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                        this.messageQueue);
                    continueConsume.set(false);
                    return;
                }
                for (MessageExt msg : msgs) {
                    MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                }
                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context,
                    currentLeftoverStageIndex);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessagePeriodicConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            } finally {
                this.processQueue.getConsumeLock().unlock();
            }
            messageListener.resetCurrentStageIfNeed(topic,
                ConsumeMessagePeriodicConcurrentlyService.this.getCurrentStageOffset(messageQueue, topic));

            if (null == status
                || ConsumeOrderlyStatus.ROLLBACK == status
                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessagePeriodicConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
            }

            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }

            if (ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext
                    .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                ConsumeMessagePeriodicConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessagePeriodicConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessagePeriodicConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
            continueConsume.set(ConsumeMessagePeriodicConcurrentlyService.this.processConsumeResult(msgs, status, context, this)
                && continueConsume.get());
        }
    }

}
