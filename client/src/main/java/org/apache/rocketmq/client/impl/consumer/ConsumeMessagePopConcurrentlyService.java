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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ConsumeMessagePopConcurrentlyService implements ConsumeMessageService {
    private static final Logger log = LoggerFactory.getLogger(ConsumeMessagePopConcurrentlyService.class);
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;

    public ConsumeMessagePopConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<>();

        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    public void start() {
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
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
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

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
            result.setRemark(UtilAll.exceptionSimpleDesc(e));

            log.warn("consumeMessageDirectly exception: {} Group: {} Msgs: {} MQ: {}",
                UtilAll.exceptionSimpleDesc(e),
                ConsumeMessagePopConcurrentlyService.this.consumerGroup,
                msgs,
                mq, e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue,
                                     MessageQueue messageQueue, boolean dispathToConsume) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void submitPopConsumeRequest(
        final List<MessageExt> msgs,
        final PopProcessQueue processQueue,
        final MessageQueue messageQueue) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest) {

        if (consumeRequest.getMsgs().isEmpty()) {
            return;
        }

        int ackIndex = context.getAckIndex();
        String topic = consumeRequest.getMessageQueue().getTopic();

        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic,
                        consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        //ack if consume success
        for (int i = 0; i <= ackIndex; i++) {
            this.defaultMQPushConsumerImpl.ackAsync(consumeRequest.getMsgs().get(i), consumerGroup);
            consumeRequest.getPopProcessQueue().ack();
        }

        //consume later if consume fail
        for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
            MessageExt msgExt = consumeRequest.getMsgs().get(i);
            consumeRequest.getPopProcessQueue().ack();
            if (msgExt.getReconsumeTimes() >= this.defaultMQPushConsumerImpl.getMaxReconsumeTimes()) {
                checkNeedAckOrDelay(msgExt);
                continue;
            }

            int delayLevel = context.getDelayLevelWhenNextConsume();
            changePopInvisibleTime(consumeRequest.getMsgs().get(i), consumerGroup, delayLevel);
        }
    }

    private void checkNeedAckOrDelay(MessageExt msgExt) {
        int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();

        long msgDelaytime = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (msgDelaytime > delayLevelTable[delayLevelTable.length - 1] * 1000 * 2) {
            log.warn("Consume too many times, ack message async. message {}", msgExt.toString());
            this.defaultMQPushConsumerImpl.ackAsync(msgExt, consumerGroup);
        } else {
            int delayLevel = delayLevelTable.length - 1;
            for (; delayLevel >= 0; delayLevel--) {
                if (msgDelaytime >= delayLevelTable[delayLevel] * 1000) {
                    delayLevel++;
                    break;
                }
            }

            changePopInvisibleTime(msgExt, consumerGroup, delayLevel);
            log.warn("Consume too many times, but delay time {} not enough. changePopInvisibleTime to delayLevel {} . message key:{}",
                msgDelaytime, delayLevel, msgExt.getKeys());
        }
    }

    private void changePopInvisibleTime(final MessageExt msg, String consumerGroup, int delayLevel) {
        if (0 == delayLevel) {
            delayLevel = msg.getReconsumeTimes();
        }

        int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();
        int delaySecond = delayLevel >= delayLevelTable.length ? delayLevelTable[delayLevelTable.length - 1] : delayLevelTable[delayLevel];
        String extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);

        try {
            this.defaultMQPushConsumerImpl.changePopInvisibleTimeAsync(msg.getTopic(), consumerGroup, extraInfo,
                    delaySecond * 1000L, new AckCallback() {
                        @Override
                        public void onSuccess(AckResult ackResult) {
                        }


                        @Override
                        public void onException(Throwable e) {
                            log.error("changePopInvisibleTimeAsync fail. msg:{} error info: {}", msg.toString(), e.toString());
                        }
                    });
        } catch (Throwable t) {
            log.error("changePopInvisibleTimeAsync fail, group:{} msg:{} errorInfo:{}", consumerGroup, msg.toString(), t.toString());
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final PopProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessagePopConcurrentlyService.this.submitPopConsumeRequest(msgs, processQueue, messageQueue);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessagePopConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final PopProcessQueue processQueue;
        private final MessageQueue messageQueue;
        private long popTime = 0;
        private long invisibleTime = 0;

        public ConsumeRequest(List<MessageExt> msgs, PopProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;

            try {
                String extraInfo = msgs.get(0).getProperty(MessageConst.PROPERTY_POP_CK);
                String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
                popTime = ExtraInfoUtil.getPopTime(extraInfoStrs);
                invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfoStrs);
            } catch (Throwable t) {
                log.error("parse extra info error. msg:" + msgs.get(0), t);
            }
        }

        public boolean isPopTimeout() {
            if (msgs.size() == 0 || popTime <= 0 || invisibleTime <= 0) {
                return true;
            }

            long current = System.currentTimeMillis();
            return current - popTime >= invisibleTime;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public PopProcessQueue getPopProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped(pop). group={} {}", ConsumeMessagePopConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            if (isPopTimeout()) {
                log.info("the pop message time out so abort consume. popTime={} invisibleTime={}, group={} {}",
                        popTime, invisibleTime, ConsumeMessagePopConcurrentlyService.this.consumerGroup, this.messageQueue);
                processQueue.decFoundMsg(-msgs.size());
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessagePopConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    UtilAll.exceptionSimpleDesc(e),
                    ConsumeMessagePopConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            }
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= invisibleTime * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessagePopConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                consumeMessageContext.setAccessChannel(defaultMQPushConsumer.getAccessChannel());
                ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessagePopConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessagePopConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            if (!processQueue.isDropped() && !isPopTimeout()) {
                ConsumeMessagePopConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                if (msgs != null) {
                    processQueue.decFoundMsg(-msgs.size());
                }

                log.warn("processQueue invalid. isDropped={}, isPopTimeout={}, messageQueue={}, msgs={}",
                        processQueue.isDropped(), isPopTimeout(), messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
