package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 并发消费消息服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ExecutorService consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();

    private final static long MaxTimeConsumeContinuously = Long.parseLong(System.getProperty(
        "rocketmq.client.maxTimeConsumeContinuously", "60000"));

    // 定时线程
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
            new ThreadFactory() {
                private AtomicLong threadIndex = new AtomicLong(0);


                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "ConsumeMessageThread-" //
                            + ConsumeMessageOrderlyService.this.consumerGroup//
                            + "-" + this.threadIndex.incrementAndGet());
                }
            });

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ConsumeMessageScheduledThread-" + consumerGroup);
            }
        });
    }


    public void start() {
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.consumeExecutor.shutdown();
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
            // 保证在当前Consumer内，同一队列串行消费
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                // 保证在Consumer集群，同一队列串行消费
                if (this.processQueue.isLocked()) {
                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume;) {
                        if (this.processQueue.isDroped()) {
                            log.info("the message queue not be able to consume, because it's droped {}",
                                this.messageQueue);
                            break;
                        }

                        if (!this.processQueue.isLocked()) {
                            // TODO 停止消费
                            break;
                        }

                        // 在线程数小于队列数情况下，防止个别队列被饿死
                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MaxTimeConsumeContinuously) {
                            // 过10ms后再消费
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
                            try {
                                status = messageListener.consumeMessage(msgs, context);
                            }
                            catch (Throwable e) {
                                log.warn("consumeMessage exception, Group: "
                                        + ConsumeMessageOrderlyService.this.consumerGroup//
                                        + " " + msgs//
                                        + " " + messageQueue, e);
                            }

                            // 用户抛出异常或者返回null，都挂起队列
                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            continueConsume =
                                    ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status,
                                        context, this);
                        }
                        else {
                            continueConsume = false;
                        }
                    }
                }
                // 没有拿到当前队列的锁，稍后再消费
                else {
                    // TODO
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
        // 非事务方式，自动提交
        if (context.isAutoCommit()) {
            switch (status) {
            case COMMIT:
            case ROLLBACK:
                log.warn(
                    "the message queue consume result is illegal, we think you want to ack these message {}",
                    consumeRequest.getMessageQueue());
            case SUCCESS:
                commitOffset = consumeRequest.getProcessQueue().commit();
                break;
            case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                this.submitConsumeRequestLater(//
                    consumeRequest.getProcessQueue(), //
                    consumeRequest.getMessageQueue(), //
                    context.getSuspendCurrentQueueTimeMillis());
                continueConsume = false;
                break;
            default:
                break;
            }
        }
        // 事务方式，由用户来控制提交回滚
        else {
            switch (status) {
            case SUCCESS:
                break;
            case COMMIT:
                commitOffset = consumeRequest.getProcessQueue().commit();
                break;
            case ROLLBACK:
                // 如果Rollback后，最好suspend一会儿再消费，防止应用无限Rollback下去
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
                break;
            default:
                break;
            }
        }

        if (commitOffset >= 0) {
            this.defaultMQPushConsumerImpl
                .updateConsumeOffset(consumeRequest.getMessageQueue(), commitOffset);
        }

        return continueConsume;
    }


    /**
     * 在Consumer本地定时线程中定时重试
     */
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
}
