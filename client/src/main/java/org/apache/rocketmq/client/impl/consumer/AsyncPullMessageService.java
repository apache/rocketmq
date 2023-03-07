package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class AsyncPullMessageService extends ServiceThread {
    private final Logger logger = LoggerFactory.getLogger(AsyncPullMessageService.class);
    private final LinkedBlockingQueue<MessageQueue> messageQueueQueue = new LinkedBlockingQueue<>();

    private final DefaultLitePullConsumerImpl consumer;
    private final ScheduledExecutorService scheduledExecutorService;

    public AsyncPullMessageService(DefaultLitePullConsumerImpl consumer) {
        this.consumer = consumer;
        this.scheduledExecutorService = Executors
                .newScheduledThreadPool(consumer.getDefaultLitePullConsumer().getPullThreadNums(), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "AsyncPullMessageServiceScheduledThread");
                    }
                });
    }

    public void executeMessageRequestLater(final MessageQueue messageQueue, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    AsyncPullMessageService.this.executeMessageRequestImmediately(messageQueue);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("AsyncPullMessageServiceScheduledThread has shutdown");
        }
    }

    public void executeMessageRequestImmediately(final MessageQueue messageQueue) {
        try {
            this.messageQueueQueue.put(messageQueue);
        } catch (InterruptedException e) {
            logger.error("executeMessageRequestImmediately pullRequestQueue.put", e);
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    private void pullMessage(final MessageQueue messageQueue) {
        consumer.pullMessage(messageQueue);
    }

    @Override
    public void run() {
        logger.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                MessageQueue messageQueue = this.messageQueueQueue.take();
                this.pullMessage(messageQueue);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                logger.error("Pull Message Service Run Method exception", e);
            }
        }

        logger.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

    public Boolean removeMessageQueue(MessageQueue messageQueue) {
        return this.messageQueueQueue.remove(messageQueue);
    }
}
