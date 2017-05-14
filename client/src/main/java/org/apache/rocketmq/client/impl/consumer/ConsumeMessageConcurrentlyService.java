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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/**
 * 并发消费消息服务
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 消息监听
     */
    private final MessageListenerConcurrently messageListener;
    /**
     * 消费线程池队列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**
     * 消费线程池
     */
    private final ThreadPoolExecutor consumeExecutor;
    /**
     * 消费分组
     */
    private final String consumerGroup;
    /**
     * 定时线程池。目前线程池大小为1。
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * 清理过期消息线程池。目前线程池大小为1。
     */
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<>();

        this.consumeExecutor = new ThreadPoolExecutor(//
            this.defaultMQPushConsumer.getConsumeThreadMin(), //
            this.defaultMQPushConsumer.getConsumeThreadMax(), //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.consumeRequestQueue, //
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0 //
            && corePoolSize <= Short.MAX_VALUE //
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        //
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}", //
        // corePoolSize,//
        // this.consumeExecutor.getCorePoolSize(),//
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        //
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}", //
        // corePoolSize,//
        // this.consumeExecutor.getCorePoolSize(),//
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    /**
     * 直接消费单条消息
     * TODO 疑问：调用方
     *
     * @param msg 消息
     * @param brokerName broker名
     * @return 消费结果
     */
    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        // 消息列表
        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);

        // 消费队列
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        // 消费Context
        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        // 当消息为重试消息，设置Topic为原始Topic
        this.resetRetryTopic(msgs);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new messge: {}", msg);

        // 执行消费
        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
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

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s", //
                RemotingHelper.exceptionSimpleDesc(e), //
                ConsumeMessageConcurrentlyService.this.consumerGroup, //
                msgs, //
                mq), e);
        }

        // 设置消费时长
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 提交消费请求
     *
     * @param msgs 消息列表
     * @param processQueue 消息处理队列
     * @param messageQueue 消息队列
     * @param dispatchToConsume 是否提交消费。目前该参数无用处。
     */
    @Override
    public void submitConsumeRequest(//
        final List<MessageExt> msgs, //
        final ProcessQueue processQueue, //
        final MessageQueue messageQueue, //
        final boolean dispatchToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) { // 提交消息小于批量消息数，直接提交消费请求
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else { // 提交消息大于批量消息数，进行分拆成多个消费请求
            for (int total = 0; total < msgs.size(); ) {
                // 计算当前拆分请求包含的消息
                List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                // 提交拆分消费请求
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    // 如果被拒绝，则将当前拆分消息+剩余消息提交延迟消费请求。
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    /**
     * 当消息为重试消息，设置Topic为原始Topic
     * 例如：%RETRY%please_rename_unique_group_name_4 =》TopicTest
     *
     * @param msgs 消息列表
     */
    public void resetRetryTopic(final List<MessageExt> msgs) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }
        }
    }

    /**
     * 清理过期消息
     */
    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * 处理消费结果
     *
     * @param status 消费结果
     * @param context 消费Context
     * @param consumeRequest 提交请求
     */
    public void processConsumeResult(//
        final ConsumeConcurrentlyStatus status, //
        final ConsumeConcurrentlyContext context, //
        final ConsumeRequest consumeRequest//
    ) {
        int ackIndex = context.getAckIndex();

        // 消息为空，直接返回
        if (consumeRequest.getMsgs().isEmpty())
            return;

        // 计算从consumeRequest.msgs[0]到consumeRequest.msgs[ackIndex]的消息消费成功
        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                // 统计成功/失败数量
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                // 统计成功/失败数量
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        // 处理消费失败的消息
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING: // 广播模式，无论是否消费失败，不发回消息到Broker，只打印Log
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                // 发回消息失败到Broker。
                List<MessageExt> msgBackFailed = new ArrayList<>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                // 发回Broker失败的消息，直接提交延迟重新消费
                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        // 移除消费成功消息，并更新最新消费进度
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 发回消息到Broker
     *
     * @param msg 消息
     * @param context 消费Context
     * @return 是否成功
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    /**
     * 提交延迟消费请求
     *
     * @param msgs 消息列表
     * @param processQueue 消息处理队列
     * @param messageQueue 消息队列
     */
    private void submitConsumeRequestLater(//
        final List<MessageExt> msgs, //
        final ProcessQueue processQueue, //
        final MessageQueue messageQueue//
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交延迟消费请求
     * @param consumeRequest 消费请求
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest//
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest); // TODO BUG ?
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {

        /**
         * 消费消息列表
         */
        private final List<MessageExt> msgs;
        /**
         * 消息处理队列
         */
        private final ProcessQueue processQueue;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            // 废弃队列不进行消费
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener; // 监听器
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue); // 消费Context
            ConsumeConcurrentlyStatus status = null; // 消费结果状态

            // Hook
            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS; // 消费返回结果类型
            try {
                // 当消息为重试消息，设置Topic为原始Topic
                ConsumeMessageConcurrentlyService.this.resetRetryTopic(msgs);

                // 设置开始消费时间
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                // 进行消费
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e), //
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            }

            // 解析消费返回结果类型
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            // Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            // 消费结果状态为空时，则设置为稍后重新消费
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            // 统计
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            // 处理消费结果
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
