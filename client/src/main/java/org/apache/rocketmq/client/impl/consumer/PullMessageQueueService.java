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

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class PullMessageQueueService extends ServiceThread {
    private final Logger logger = LoggerFactory.getLogger(PullMessageQueueService.class);
    private final LinkedBlockingQueue<MessageQueue> messageQueueQueue = new LinkedBlockingQueue<>();

    private final DefaultLitePullConsumerImpl consumer;
    private final ScheduledExecutorService scheduledExecutorService;

    public PullMessageQueueService(DefaultLitePullConsumerImpl consumer) {
        this.consumer = consumer;
        this.scheduledExecutorService = Executors
                .newScheduledThreadPool(consumer.getDefaultLitePullConsumer().getPullThreadNums(), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "PullMessageQueueServiceScheduledThread");
                    }
                });
    }

    public void executeMessageRequestLater(final MessageQueue messageQueue, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageQueueService.this.executeMessageRequestImmediately(messageQueue);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("PullMessageQueueServiceScheduledThread has shutdown");
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
                logger.error("Pull Message Queue Service Run Method exception", e);
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
