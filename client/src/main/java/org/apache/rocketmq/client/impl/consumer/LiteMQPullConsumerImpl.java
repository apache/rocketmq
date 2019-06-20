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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

public class LiteMQPullConsumerImpl extends DefaultMQPullConsumerImpl {
    private final InternalLogger log = ClientLogger.getLog();

    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

    private List<ConsumeRequest> allConsumed = new ArrayList<ConsumeRequest>(256);

    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();
    ;

    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public LiteMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer, final RPCHook rpcHook) {
        super(defaultMQPullConsumer, rpcHook);
    }

    public void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(assignedMessageQueue);
        updatePullTask(topic, assignedMessageQueue);
    }

    public void updatePullTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }

        for (MessageQueue messageQueue : mqNewSet) {
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel = defaultMQPullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    updateAssignedMessageQueue(topic, mqAll);
                    break;
                case CLUSTERING:
                    updateAssignedMessageQueue(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    int nextPullBatchNums() {
        return Math.min(10, consumeRequestCache.remainingCapacity());
    }

    @Override
    public synchronized void start() throws MQClientException {
        super.start();
        final String group = this.defaultMQPullConsumer.getConsumerGroup();
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            10, //this.pullThreadNums,
            new ThreadFactoryImpl("PullMsgThread-" + group)
        );
        this.defaultMQPullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
    }

    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(defaultMQPullConsumer.getConsumerGroup(),
                topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    void updatePullOffset(MessageQueue remoteQueue, long nextPullOffset) {
        try {
            assignedMessageQueue.updateNextOffset(remoteQueue, nextPullOffset);
        } catch (MQClientException e) {
            log.error("A error occurred in update consume: {} offset process.", remoteQueue, e);
        }
    }

    private void addToConsumed(ConsumeRequest consumeRequest) {
        synchronized (this.allConsumed) {
            allConsumed.add(consumeRequest);
        }
    }

    void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
            addToConsumed(consumeRequest);
        } catch (InterruptedException ex) {
            log.error("Submit consumeRequest error", ex);
        }
    }

    long nextPullOffset(MessageQueue remoteQueue) {
        long offset = -1;
        try {
            offset = assignedMessageQueue.getNextOffset(remoteQueue);
            if (offset == -1) {
                offset = this.defaultMQPullConsumer.fetchConsumeOffset(remoteQueue, false);
                assignedMessageQueue.updateNextOffset(remoteQueue, offset);
            }
        } catch (MQClientException e) {
            log.error("An error occurred in fetch consume offset process.", e);
        }
        return offset;
    }

    public class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;
        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            String topic = this.messageQueue.getTopic();
            if (!this.isCancelled()) {
                if (assignedMessageQueue.isPaused(messageQueue)) {
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }
                SubscriptionData subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                long offset = nextPullOffset(messageQueue);
                try {
                    PullResult pullResult = defaultMQPullConsumer.pull(messageQueue, subscriptionData.getSubString(), offset, nextPullBatchNums());
                    ProcessQueue processQueue = rebalanceImpl.getProcessQueueTable().get(messageQueue);
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            if (processQueue != null) {
                                processQueue.putMessage(pullResult.getMsgFoundList());
                                submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                            }
                            break;
                        default:
                            break;
                    }
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset());
                } catch (Exception e) {
                    log.error("An error occurred in pull message process.", e);
                }
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }

    public class ConsumeRequest {
        private final List<MessageExt> messageExts;
        private final MessageQueue messageQueue;
        private final ProcessQueue processQueue;
        private long startConsumeTimeMillis;

        public ConsumeRequest(final List<MessageExt> messageExts, final MessageQueue messageQueue,
            final ProcessQueue processQueue) {
            this.messageExts = messageExts;
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public List<MessageExt> getMessageExts() {
            return messageExts;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public long getStartConsumeTimeMillis() {
            return startConsumeTimeMillis;
        }

        public void setStartConsumeTimeMillis(final long startConsumeTimeMillis) {
            this.startConsumeTimeMillis = startConsumeTimeMillis;
        }
    }
}
