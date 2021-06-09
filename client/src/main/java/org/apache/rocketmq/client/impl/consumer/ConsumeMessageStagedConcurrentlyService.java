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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.ConsumeStagedConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListenerStagedConcurrently;
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

public class ConsumeMessageStagedConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerStagedConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor dispatchExecutor;
    private final ThreadPoolExecutor consumeExecutor;
    private final PriorityConcurrentEngine engine;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;
    private final Map<String/*strategyId*/, List<Integer>/*StageDefinition*/> summedStageDefinitionMap;
    private final ConcurrentMap<String/*topic*/, ConcurrentMap<String/*strategyId*/, AtomicInteger/*currentStageOffset*/>> currentStageOffsetMap = new ConcurrentHashMap<>();
    private final int pullBatchSize;
    private final StageOffsetStore stageOffsetStore;

    public ConsumeMessageStagedConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerStagedConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;
        this.summedStageDefinitionMap = new HashMap<>();
        Map<String, List<Integer>> strategies = messageListener.getStageDefinitionStrategies();
        if (MapUtils.isNotEmpty(strategies)) {
            for (Map.Entry<String, List<Integer>> entry : strategies.entrySet()) {
                String strategyId = entry.getKey();
                List<Integer> definitions = entry.getValue();
                List<Integer> summedStageDefinitions = new ArrayList<>();
                if (definitions != null) {
                    int sum = 0;
                    for (Integer stageDefinition : definitions) {
                        summedStageDefinitions.add(sum = sum + stageDefinition);
                    }
                }
                summedStageDefinitionMap.put(strategyId, summedStageDefinitions);
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
        if (MessageModel.CLUSTERING.equals(ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessageStagedConcurrentlyService.this.lockMQPeriodically();
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

    public AtomicInteger getCurrentStageOffset(MessageQueue messageQueue, String topic, String strategyId) {
        if (null == strategyId) {
            return new AtomicInteger(-1);
        }
        ConcurrentMap<String, AtomicInteger> indexs = currentStageOffsetMap.get(topic);
        if (null == indexs) {
            ConcurrentMap<String, AtomicInteger> stageOffset = stageOffsetStore == null ?
                new ConcurrentHashMap<>() : convert(stageOffsetStore.readStageOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            currentStageOffsetMap.putIfAbsent(topic, stageOffset);
            indexs = currentStageOffsetMap.get(topic);
        }
        indexs.putIfAbsent(strategyId, new AtomicInteger(0));
        return indexs.get(strategyId);
    }

    private ConcurrentMap<String, AtomicInteger> convert(Map<String, Integer> original) {
        if (null == original) {
            return new ConcurrentHashMap<>();
        }
        ConcurrentMap<String, AtomicInteger> map = new ConcurrentHashMap<>(original.size());
        for (Map.Entry<String, Integer> entry : original.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();
            map.put(key, new AtomicInteger(value));
        }
        return map;
    }

    public int getCurrentLeftoverStage(MessageQueue messageQueue, String topic, String strategyId) {
        List<Integer> summedStageDefinition = summedStageDefinitionMap.get(strategyId);
        if (CollectionUtils.isNotEmpty(summedStageDefinition)) {
            for (Integer stageDefinition : summedStageDefinition) {
                int left = stageDefinition - getCurrentStageOffset(messageQueue, topic, strategyId).get();
                if (left > 0) {
                    return left;
                }
            }
        }
        return -1;
    }

    public int getCurrentLeftoverStageIndex(MessageQueue messageQueue, String topic, String strategyId) {
        List<Integer> summedStageDefinition = summedStageDefinitionMap.get(strategyId);
        if (CollectionUtils.isNotEmpty(summedStageDefinition)) {
            for (int i = 0; i < summedStageDefinition.size(); i++) {
                int left = summedStageDefinition.get(i) - getCurrentStageOffset(messageQueue, topic, strategyId).get();
                if (left > 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    public int getCurrentLeftoverStageIndexAndUpdate(MessageQueue messageQueue, String topic, String strategyId) {
        return getCurrentLeftoverStageIndexAndUpdate(messageQueue, topic, strategyId, 1);
    }

    public int getCurrentLeftoverStageIndexAndUpdate(MessageQueue messageQueue, String topic, String strategyId,
        int delta) {
        final AtomicInteger offset = getCurrentStageOffset(messageQueue, topic, strategyId);
        synchronized (offset) {
            try {
                return getCurrentLeftoverStageIndex(messageQueue, topic, strategyId);
            } finally {
                offset.getAndAdd(delta);
            }
        }
    }

    public int decrementCurrentStageOffset(MessageQueue messageQueue, String topic, String strategyId) {
        return decrementCurrentStageOffset(messageQueue, topic, strategyId, 1);
    }

    public int decrementCurrentStageOffset(MessageQueue messageQueue, String topic, String strategyId, int delta) {
        final AtomicInteger offset = getCurrentStageOffset(messageQueue, topic, strategyId);
        synchronized (offset) {
            return offset.getAndSet(offset.get() - delta);
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

        ConsumeStagedConcurrentlyContext context = new ConsumeStagedConcurrentlyContext(mq);

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
            String strategyId = null;
            try {
                strategyId = this.messageListener.computeStrategy(msg);
            } catch (Exception ignored) {
            }
            if (StringUtils.isNotBlank(strategyId)) {
                int stageIndex = this.getCurrentLeftoverStageIndexAndUpdate(messageQueue, topic, strategyId);
                context.setStrategyId(strategyId);
                context.setStageIndex(stageIndex);
            }
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
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
                        decrementCurrentStageOffset(messageQueue, topic, strategyId);
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
                ConsumeMessageStagedConcurrentlyService.this.consumerGroup,
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
                boolean lockOK = ConsumeMessageStagedConcurrentlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessageStagedConcurrentlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageStagedConcurrentlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
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
                ConsumeMessageStagedConcurrentlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
        final String strategyId,
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeStagedConcurrentlyContext context,
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
                    commitStageOffset = getCurrentStageOffset(messageQueue, topic, strategyId).get();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    decrementCurrentStageOffset(messageQueue, topic, strategyId, msgs.size());
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
                        commitStageOffset = getCurrentStageOffset(messageQueue, topic, strategyId).get();
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
                    commitStageOffset = getCurrentStageOffset(messageQueue, topic, strategyId).get();
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
                    decrementCurrentStageOffset(messageQueue, topic, strategyId, msgs.size());
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
            stageOffsetStore.updateStageOffset(messageQueue, strategyId, commitStageOffset, false);
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
                if (MessageModel.BROADCASTING.equals(ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (final AtomicBoolean continueConsume = new AtomicBoolean(true); continueConsume.get(); ) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageStagedConcurrentlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageStagedConcurrentlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageStagedConcurrentlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize =
                            ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
                        int takeSize = ConsumeMessageStagedConcurrentlyService.this.pullBatchSize * consumeBatchSize;
                        List<MessageExt> msgs = this.processQueue.takeMessages(takeSize);
                        if (!msgs.isEmpty()) {
                            Map<String, List<MessageExt>> messageGroupByStrategyId = removeAndRePutAllMessagesInTheNextStage(topic, msgs);
                            Map<String, List<List<MessageExt>>> messagesCanConsume = UtilAll.partition(messageGroupByStrategyId, consumeBatchSize);
                            for (Map.Entry<String, List<List<MessageExt>>> entry : messagesCanConsume.entrySet()) {
                                String strategyId = entry.getKey();
                                List<List<MessageExt>> lists = entry.getValue();
                                for (final List<MessageExt> list : lists) {
                                    defaultMQPushConsumerImpl.resetRetryAndNamespace(list, defaultMQPushConsumer.getConsumerGroup());
                                    int currentLeftoverStageIndex =
                                        ConsumeMessageStagedConcurrentlyService.this.getCurrentLeftoverStageIndexAndUpdate(this.messageQueue, topic, strategyId, list.size());
                                    ConsumeRequest consumeRequest = new ConsumeRequest(list, this.processQueue, this.messageQueue, continueConsume, currentLeftoverStageIndex, strategyId);
                                    if (currentLeftoverStageIndex >= 0) {
                                        engine.runPriorityAsync(currentLeftoverStageIndex, consumeRequest);
                                    } else {
                                        //strategyId为null也会走这个case
                                        engine.runPriorityAsync(consumeRequest);
                                    }
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

                    ConsumeMessageStagedConcurrentlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

        private Map<String, List<MessageExt>> removeAndRePutAllMessagesInTheNextStage(String topic, List<MessageExt> msgs) {
            Map<String, List<MessageExt>> messageGroupByStrategyId = new LinkedHashMap<>();
            for (MessageExt message : msgs) {
                String strategyId = null;
                try {
                    strategyId = messageListener.computeStrategy(message);
                } catch (Exception e) {
                    throw new RuntimeException("computeStrategy failed with exception:" + e.getMessage() + " !");
                }
                //null的key，表示直接走并发
                List<MessageExt> messages = messageGroupByStrategyId.putIfAbsent(strategyId, new CopyOnWriteArrayList<>());
                if (null == messages) {
                    messages = messageGroupByStrategyId.get(strategyId);
                }
                messages.add(message);
            }
            for (Map.Entry<String, List<MessageExt>> entry : messageGroupByStrategyId.entrySet()) {
                String strategyId = entry.getKey();
                List<MessageExt> messages = entry.getValue();
                int leftoverStage = ConsumeMessageStagedConcurrentlyService.this.getCurrentLeftoverStage(this.messageQueue, topic, strategyId);
                int size = messages.size();
                if (leftoverStage < 0 || size <= leftoverStage) {
                    continue;
                }
                List<MessageExt> list = messages.subList(leftoverStage, size);
                //the messages must be put back here
                this.processQueue.putMessage(list);
                messages.removeAll(list);
            }
            return messageGroupByStrategyId;
        }
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;
        private final AtomicBoolean continueConsume;
        private final int currentLeftoverStageIndex;
        private final String strategyId;

        public ConsumeRequest(List<MessageExt> msgs,
            ProcessQueue processQueue,
            MessageQueue messageQueue,
            AtomicBoolean continueConsume,
            int currentLeftoverStage,
            String strategyId) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
            this.continueConsume = continueConsume;
            this.currentLeftoverStageIndex = currentLeftoverStage;
            this.strategyId = strategyId;
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
            ConsumeStagedConcurrentlyContext context = new ConsumeStagedConcurrentlyContext(this.messageQueue);
            context.setStageIndex(currentLeftoverStageIndex);
            ConsumeOrderlyStatus status = null;

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext
                    .setConsumerGroup(ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                // init the consume context type
                consumeMessageContext.setProps(new HashMap<String, String>());
                ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
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
                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageStagedConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            } finally {
                this.processQueue.getConsumeLock().unlock();
            }
            messageListener.resetCurrentStageOffsetIfNeed(topic, strategyId,
                ConsumeMessageStagedConcurrentlyService.this.getCurrentStageOffset(messageQueue, topic, strategyId));

            if (null == status
                || ConsumeOrderlyStatus.ROLLBACK == status
                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageStagedConcurrentlyService.this.consumerGroup,
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

            if (ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }

            if (ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext
                    .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                ConsumeMessageStagedConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageStagedConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageStagedConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
            continueConsume.set(ConsumeMessageStagedConcurrentlyService.this.processConsumeResult(strategyId, msgs, status, context, this)
                && continueConsume.get());
        }
    }

}
