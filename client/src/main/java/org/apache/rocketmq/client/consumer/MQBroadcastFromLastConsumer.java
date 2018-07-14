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
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Schedule service for pull consumer,which use broadcast model  and consume message from last offset
 */
public class MQBroadcastFromLastConsumer {
    private final Logger log = ClientLogger.getLog();

    private final MessageQueueListener messageQueueListener = new MessageQueueListenerImpl();
    private final ConcurrentHashMap<MessageQueue, PullTaskImpl> taskTable =
            new ConcurrentHashMap<MessageQueue, PullTaskImpl>();
    private DefaultMQPullConsumer defaultMQPullConsumer;
    private int pullThreadNums;
    private ConcurrentHashMap<String /* topic */, PullTaskCallback> callbackTable =
            new ConcurrentHashMap<String, PullTaskCallback>();
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private Executor rebalanceExecutor = Executors.newSingleThreadExecutor();

    public MQBroadcastFromLastConsumer(final String consumerGroup, int pullThreadNums) {
        this.pullThreadNums = pullThreadNums;
        this.defaultMQPullConsumer = new DefaultMQPullConsumer(consumerGroup);
        this.defaultMQPullConsumer.setMessageModel(MessageModel.BROADCASTING);
    }

    public void putTask(final String topic, final Set<MessageQueue> mqNewSet) {
        rebalanceExecutor.execute(new Runnable() {
            @Override
            public void run() {
                Iterator<Entry<MessageQueue, PullTaskImpl>> it = MQBroadcastFromLastConsumer.this.taskTable.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<MessageQueue, PullTaskImpl> next = it.next();
                    if (next.getKey().getTopic().equals(topic)) {
                        if (!mqNewSet.contains(next.getKey())) {
                            next.getValue().setCancelled(true);
                            it.remove();
                        }
                    }
                }

                for (MessageQueue mq : mqNewSet) {
                    if (!taskTable.containsKey(mq)) {
                        long offset = 0;
                        try {
                            offset = defaultMQPullConsumer.maxOffset(mq);
                        } catch (MQClientException e) {
                            log.error("get max offset error:{}", e.getMessage());
                        }
                        if (offset < 0) {
                            offset = 0;
                        }

                        MQBroadcastFromLastConsumer.this.defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getOffsetStore().updateOffset(mq, offset, false);
                        PullTaskImpl command = new PullTaskImpl(mq);
                        MQBroadcastFromLastConsumer.this.taskTable.put(mq, command);
                        MQBroadcastFromLastConsumer.this.scheduledThreadPoolExecutor.schedule(command, 0, TimeUnit.MILLISECONDS);
                    }
                }
            }
        });
    }

    public void start() throws MQClientException {
        final String group = this.defaultMQPullConsumer.getConsumerGroup();
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
                this.pullThreadNums,
                new ThreadFactoryImpl("PullMsgThread-" + group)
        );

        this.defaultMQPullConsumer.setMessageQueueListener(this.messageQueueListener);

        this.defaultMQPullConsumer.start();

        log.info("MQBroadcastFromLastConsumer start OK, {} {}",
                this.defaultMQPullConsumer.getConsumerGroup(), this.callbackTable);
    }

    public void registerPullTaskCallback(final String topic, final PullTaskCallback callback) {
        this.callbackTable.put(topic, callback);
        this.defaultMQPullConsumer.registerMessageQueueListener(topic, null);
    }

    public void shutdown() {
        if (this.scheduledThreadPoolExecutor != null) {
            this.scheduledThreadPoolExecutor.shutdown();
        }

        if (this.defaultMQPullConsumer != null) {
            this.defaultMQPullConsumer.shutdown();
        }
    }

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            log.info("messageQueueChanged,topic:[{}]", topic);
            MessageModel messageModel =
                    MQBroadcastFromLastConsumer.this.defaultMQPullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    MQBroadcastFromLastConsumer.this.putTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    MQBroadcastFromLastConsumer.this.putTask(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.defaultMQPullConsumer.setNamesrvAddr(namesrvAddr);
    }

    public void setInstanceName(String instanceName) {
        this.defaultMQPullConsumer.setInstanceName(instanceName);
    }

    class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;

        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            //log.debug("run pull task, if cancel:{}", this.cancelled);
            String topic = this.messageQueue.getTopic();
            if (!this.isCancelled()) {
                PullTaskCallback pullTaskCallback =
                        MQBroadcastFromLastConsumer.this.callbackTable.get(topic);
                if (pullTaskCallback != null) {
                    final PullTaskContext context = new PullTaskContext();
                    context.setPullConsumer(MQBroadcastFromLastConsumer.this.defaultMQPullConsumer);
                    try {
                        pullTaskCallback.doPullTask(this.messageQueue, context);
                    } catch (Throwable e) {
                        context.setPullNextDelayTimeMillis(1000);
                        log.error("doPullTask Exception", e);
                    }

                    if (!this.isCancelled()) {
                        MQBroadcastFromLastConsumer.this.scheduledThreadPoolExecutor.schedule(this,
                                context.getPullNextDelayTimeMillis(), TimeUnit.MILLISECONDS);
                    } else {
                        log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                    }
                } else {
                    log.warn("Pull Task Callback not exist , {}", topic);
                }
            } else {
                log.warn("The Pull Task is cancelled, {}", messageQueue);
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }
    }
}
