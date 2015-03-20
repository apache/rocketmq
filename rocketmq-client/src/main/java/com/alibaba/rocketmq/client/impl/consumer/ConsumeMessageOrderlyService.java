/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.CMResult;
import com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;


/**
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-27
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final static long MaxTimeConsumeContinuously = Long.parseLong(System.getProperty(
        "rocketmq.client.maxTimeConsumeContinuously", "60000"));

    private volatile boolean stopped = false;

    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();

    private final ScheduledExecutorService scheduledExecutorService;


    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(//
            this.defaultMQPushConsumer.getConsumeThreadMin(),//
            this.defaultMQPushConsumer.getConsumeThreadMax(),//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.consumeRequestQueue,//
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
                    "ConsumeMessageScheduledThread_"));
    }


    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl
            .messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.RebalanceLockInterval, TimeUnit.MILLISECONDS);
        }
    }


    public void shutdown() {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }


    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }


    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }


    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }


    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
            final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                }
                else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }


    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;


        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }


        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}",
                    this.messageQueue);
                return;
            }

            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                if (MessageModel.BROADCASTING
                    .equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume;) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}",
                                this.messageQueue);
                            break;
                        }

                        if (MessageModel.CLUSTERING
                            .equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl
                                .messageModel())
                                && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue,
                                this.processQueue, 10);
                            break;
                        }

                        if (MessageModel.CLUSTERING
                            .equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl
                                .messageModel())
                                && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}",
                                this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue,
                                this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MaxTimeConsumeContinuously) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue,
                                messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize =
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumer
                                    .getConsumeMessageBatchMaxSize();

                        List<MessageExt> msgs = this.processQueue.takeMessags(consumeBatchSize);
                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context =
                                    new ConsumeOrderlyContext(this.messageQueue);

                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                    .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer
                                        .getConsumerGroup());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl
                                    .executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();

                            try {
                                this.processQueue.getLockConsume().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn(
                                        "consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                        this.messageQueue);
                                    break;
                                }

                                status =
                                        messageListener.consumeMessage(Collections.unmodifiableList(msgs),
                                            context);
                            }
                            catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",//
                                    RemotingHelper.exceptionSimpleDesc(e),//
                                    ConsumeMessageOrderlyService.this.consumerGroup,//
                                    msgs,//
                                    messageQueue);
                            }
                            finally {
                                this.processQueue.getLockConsume().unlock();
                            }

                            if (null == status //
                                    || ConsumeOrderlyStatus.ROLLBACK == status//
                                    || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",//
                                    ConsumeMessageOrderlyService.this.consumerGroup,//
                                    msgs,//
                                    messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext.setSuccess(ConsumeOrderlyStatus.SUCCESS == status
                                        || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl
                                    .executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager().incConsumeRT(
                                ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(),
                                consumeRT);

                            continueConsume =
                                    ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status,
                                        context, this);
                        }
                        else {
                            continueConsume = false;
                        }
                    }
                }
                else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}",
                            this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue,
                        this.processQueue, 100);
                }
            }
        }


        public ProcessQueue getProcessQueue() {
            return processQueue;
        }


        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }


    public boolean processConsumeResult(//
            final List<MessageExt> msgs, //
            final ConsumeOrderlyStatus status, //
            final ConsumeOrderlyContext context, //
            final ConsumeRequest consumeRequest//
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        if (context.isAutoCommit()) {
            switch (status) {
            case COMMIT:
            case ROLLBACK:
                log.warn(
                    "the message queue consume result is illegal, we think you want to ack these message {}",
                    consumeRequest.getMessageQueue());
            case SUCCESS:
                commitOffset = consumeRequest.getProcessQueue().commit();
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup,
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                this.submitConsumeRequestLater(//
                    consumeRequest.getProcessQueue(), //
                    consumeRequest.getMessageQueue(), //
                    context.getSuspendCurrentQueueTimeMillis());
                continueConsume = false;

                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            default:
                break;
            }
        }
        else {
            switch (status) {
            case SUCCESS:
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup,
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            case COMMIT:
                commitOffset = consumeRequest.getProcessQueue().commit();
                break;
            case ROLLBACK:
                consumeRequest.getProcessQueue().rollback();
                this.submitConsumeRequestLater(//
                    consumeRequest.getProcessQueue(), //
                    consumeRequest.getMessageQueue(), //
                    context.getSuspendCurrentQueueTimeMillis());
                continueConsume = false;
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                this.submitConsumeRequestLater(//
                    consumeRequest.getProcessQueue(), //
                    consumeRequest.getMessageQueue(), //
                    context.getSuspendCurrentQueueTimeMillis());
                continueConsume = false;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                    consumeRequest.getMessageQueue().getTopic(), msgs.size());
                break;
            default:
                break;
            }
        }

        if (commitOffset >= 0) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(),
                commitOffset, false);
        }

        return continueConsume;
    }

    private void submitConsumeRequestLater(//
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue,//
            final long suspendTimeMillis//
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis < 10) {
            timeMillis = 10;
        }
        else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageOrderlyService.this
                    .submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }


    @Override
    public void submitConsumeRequest(//
            final List<MessageExt> msgs, //
            final ProcessQueue processQueue, //
            final MessageQueue messageQueue, //
            final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
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

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new messge: {}", msg);

        try {
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
                    result.setConsumeResult(CMResult.CR_LATER);
                    break;
                default:
                    break;
                }
            }
            else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        }
        catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",//
                RemotingHelper.exceptionSimpleDesc(e),//
                ConsumeMessageOrderlyService.this.consumerGroup,//
                msgs,//
                mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

}
