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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.MessageQueue;

public class AssignedMessageQueue {

    private ConcurrentHashMap<MessageQueue, MessageQueueStat> assignedMessageQueueState;

    private RebalanceImpl rebalanceImpl;

    public AssignedMessageQueue() {
        assignedMessageQueueState = new ConcurrentHashMap<MessageQueue, MessageQueueStat>();
    }

    public void setRebalanceImpl(RebalanceImpl rebalanceImpl) {
        this.rebalanceImpl = rebalanceImpl;
    }

    public Set<MessageQueue> messageQueues() {
        return assignedMessageQueueState.keySet();
    }

    public boolean isPaused(MessageQueue messageQueue) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            return messageQueueStat.isPaused();
        }
        return true;
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        for (MessageQueue messageQueue : messageQueues) {
            MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueStat.getPausedLatch().reset();
                messageQueueStat.setPaused(true);
            }
        }
    }

    public void resume(Collection<MessageQueue> messageQueueCollection) {
        for (MessageQueue messageQueue : messageQueueCollection) {
            MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueStat.setPaused(false);
                messageQueueStat.getPausedLatch().reset();
            }
        }
    }

    public ProcessQueue getProcessQueue(MessageQueue messageQueue) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            return messageQueueStat.getProcessQueue();
        }
        return null;
    }

    public long getPullOffset(MessageQueue messageQueue) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            return messageQueueStat.getPullOffset();
        }
        return -1;
    }

    public void updatePullOffset(MessageQueue messageQueue, long offset) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            messageQueueStat.setPullOffset(offset);
        }
    }

    public long getConusmerOffset(MessageQueue messageQueue) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            return messageQueueStat.getConsumeOffset();
        }
        return -1;
    }

    public void updateConsumeOffset(MessageQueue messageQueue, long offset) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            messageQueueStat.setConsumeOffset(offset);
        }
    }

    public void setSeekOffset(MessageQueue messageQueue, long offset) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            messageQueueStat.setSeekOffset(offset);
        }
    }

    public long getSeekOffset(MessageQueue messageQueue) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            return messageQueueStat.getSeekOffset();
        }
        return -1;
    }

    public CountDownLatch2 getPausedLatch(MessageQueue messageQueue) {
        MessageQueueStat messageQueueStat = assignedMessageQueueState.get(messageQueue);
        if (messageQueueStat != null) {
            return messageQueueStat.getPausedLatch();
        }
        return null;
    }

    public void updateAssignedMessageQueue(String topic, Collection<MessageQueue> assigned) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueStat>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueStat> next = it.next();
                if (next.getKey().getTopic().equals(topic)) {
                    if (!assigned.contains(next.getKey())) {
                        next.getValue().getProcessQueue().setDropped(true);
                        it.remove();
                    }
                }
            }
            addAssignedMessageQueue(assigned);
        }
    }

    public void updateAssignedMessageQueue(Collection<MessageQueue> assigned) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueStat>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueStat> next = it.next();
                if (!assigned.contains(next.getKey())) {
                    next.getValue().getProcessQueue().setDropped(true);
                    it.remove();
                }
            }
            addAssignedMessageQueue(assigned);
        }
    }

    private void addAssignedMessageQueue(Collection<MessageQueue> assigned) {
        for (MessageQueue messageQueue : assigned) {
            if (!this.assignedMessageQueueState.containsKey(messageQueue)) {
                MessageQueueStat messageQueueStat;
                if (rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().get(messageQueue) != null) {
                    messageQueueStat = new MessageQueueStat(messageQueue, rebalanceImpl.getProcessQueueTable().get(messageQueue));
                } else {
                    ProcessQueue processQueue = new ProcessQueue();
                    messageQueueStat = new MessageQueueStat(messageQueue, processQueue);
                }
                this.assignedMessageQueueState.put(messageQueue, messageQueueStat);
            }
        }
    }

    public void removeAssignedMessageQueue(String topic) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueStat>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueStat> next = it.next();
                if (next.getKey().getTopic().equals(topic)) {
                    it.remove();
                }
            }
        }
    }

    public class MessageQueueStat {
        private MessageQueue messageQueue;
        private ProcessQueue processQueue;
        private volatile boolean paused = false;
        private volatile long pullOffset = -1;
        private volatile long consumeOffset = -1;
        private volatile long seekOffset = -1;
        private CountDownLatch2 pausedLatch = new CountDownLatch2(1);

        public MessageQueueStat(MessageQueue messageQueue, ProcessQueue processQueue) {
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        public boolean isPaused() {
            return paused;
        }

        public void setPaused(boolean paused) {
            this.paused = paused;
        }

        public long getPullOffset() {
            return pullOffset;
        }

        public void setPullOffset(long pullOffset) {
            this.pullOffset = pullOffset;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public void setProcessQueue(ProcessQueue processQueue) {
            this.processQueue = processQueue;
        }

        public long getConsumeOffset() {
            return consumeOffset;
        }

        public void setConsumeOffset(long consumeOffset) {
            this.consumeOffset = consumeOffset;
        }

        public long getSeekOffset() {
            return seekOffset;
        }

        public void setSeekOffset(long seekOffset) {
            this.seekOffset = seekOffset;
        }

        public CountDownLatch2 getPausedLatch() {
            return pausedLatch;
        }
    }
}
