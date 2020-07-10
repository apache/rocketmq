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
package org.apache.rocketmq.common.rebalance;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.rocketmq.common.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class AllocateMessageQueueSticky implements AllocateMessageQueueStrategy {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final Map<String, List<MessageQueue>> messageQueueAllocation = new HashMap<String, List<MessageQueue>>();

    private final List<MessageQueue> unassignedQueues = new ArrayList<MessageQueue>();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID,
        List<MessageQueue> mqAll, final List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        // Rebalance (or fresh assignment) needed
        if (cidAll.size() != messageQueueAllocation.size() || mqAll.size() != getPrevMqAll().size()) {
            // Update messageQueueAllocation
            updateMessageQueueAllocation(mqAll, cidAll);

            // Sort consumers based on how many message queues are assigned to them
            TreeSet<String> sortedSubscriptions = new TreeSet<String>(new ConsumerComparator(messageQueueAllocation));
            sortedSubscriptions.addAll(messageQueueAllocation.keySet());

            // Assign unassignedQueues to consumers so that queues allocations are as balanced as possible
            for (MessageQueue mq : unassignedQueues) {
                String consumer = sortedSubscriptions.first();
                sortedSubscriptions.remove(consumer);
                messageQueueAllocation.get(consumer).add(mq);
                sortedSubscriptions.add(consumer);
            }
            unassignedQueues.clear();

            // Reassignment until no message queue can be moved to improve the balance
            Map<String, List<MessageQueue>> preBalanceAllocation = new HashMap<String, List<MessageQueue>>(messageQueueAllocation);
            while (!isBalanced(sortedSubscriptions)) {
                String leastSubscribedConsumer = sortedSubscriptions.first();
                String mostSubscribedConsumer = sortedSubscriptions.last();
                MessageQueue mqFromMostSubscribedConsumer = messageQueueAllocation.get(mostSubscribedConsumer).get(0);
                messageQueueAllocation.get(leastSubscribedConsumer).add(mqFromMostSubscribedConsumer);
                messageQueueAllocation.get(mostSubscribedConsumer).remove(mqFromMostSubscribedConsumer);
            }

            // Make sure it is getting a more balanced allocation than before; otherwise, revert to previous allocation
            if (getBalanceScore(messageQueueAllocation) >= getBalanceScore(preBalanceAllocation)) {
                deepCopy(preBalanceAllocation, messageQueueAllocation);
            }
        }

        return messageQueueAllocation.get(currentCID);
    }

    private void updateMessageQueueAllocation(List<MessageQueue> mqAll, List<String> cidAll) {
        // The current size of consumers is larger than before
        if (cidAll.size() > messageQueueAllocation.size()) {
            for (String cid : cidAll) {
                if (!messageQueueAllocation.containsKey(cid)) {
                    messageQueueAllocation.put(cid, new ArrayList<MessageQueue>());
                }
            }
        }

        // The current size of consumers is smaller than before
        if (cidAll.size() < messageQueueAllocation.size()) {
            Iterator<Map.Entry<String, List<MessageQueue>>> it = messageQueueAllocation.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, List<MessageQueue>> entry = it.next();
                if (!cidAll.contains(entry.getKey())) {
                    it.remove();
                }
            }
        }

        // The current size of message queues is larger than before
        List<MessageQueue> prevMqAll = getPrevMqAll();
        if (mqAll.size() > prevMqAll.size()) {
            for (MessageQueue mq : mqAll) {
                if (!prevMqAll.contains(mq)) {
                    unassignedQueues.add(mq);
                }
            }
        }

        // The current size of message queues is smaller than before
        if (mqAll.size() < prevMqAll.size()) {
            for (MessageQueue prevMq : prevMqAll) {
                if (!isSameQueueIdExists(mqAll, prevMq.getQueueId())) {
                    for (List<MessageQueue> prevMqs : messageQueueAllocation.values()) {
                        prevMqs.remove(prevMq);
                    }
                }
            }
        }
    }

    private static class ConsumerComparator implements Comparator<String>, Serializable {
        private final Map<String, List<MessageQueue>> map;

        ConsumerComparator(Map<String, List<MessageQueue>> map) {
            this.map = map;
        }

        @Override
        public int compare(String o1, String o2) {
            int ret = map.get(o1).size() - map.get(o2).size();
            if (ret == 0) {
                ret = o1.compareTo(o2);
            }
            return ret;
        }
    }

    private boolean isSameQueueIdExists(List<MessageQueue> mqAll, int prevMqId) {
        for (MessageQueue mq : mqAll) {
            if (mq.getQueueId() == prevMqId) {
                return true;
            }
        }
        return false;
    }

    private boolean isBalanced(TreeSet<String> sortedCurrentSubscriptions) {
        int min = this.messageQueueAllocation.get(sortedCurrentSubscriptions.first()).size();
        int max = this.messageQueueAllocation.get(sortedCurrentSubscriptions.last()).size();
        // if minimum and maximum numbers of message queues allocated to consumers differ by at most 1
        return min >= max - 1;
    }

    /**
     * @return The balance score of the given allocation, which is the sum of assigned queues size difference of all
     * consumer. A well balanced allocation with balance score of 0 (all consumers getting the same number of
     * allocations). Lower balance score represents a more balanced allocation.
     */
    private int getBalanceScore(Map<String, List<MessageQueue>> allocation) {
        int score = 0;

        Map<String, Integer> consumerAllocationSizes = new HashMap<String, Integer>();
        for (Map.Entry<String, List<MessageQueue>> entry : allocation.entrySet()) {
            consumerAllocationSizes.put(entry.getKey(), entry.getValue().size());
        }

        Iterator<Map.Entry<String, Integer>> it = consumerAllocationSizes.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Integer> entry = it.next();
            int consumerAllocationSize = entry.getValue();
            it.remove();
            for (Map.Entry<String, Integer> otherEntry : consumerAllocationSizes.entrySet()) {
                score += Math.abs(consumerAllocationSize - otherEntry.getValue());
            }
        }

        return score;
    }

    private void deepCopy(Map<String, List<MessageQueue>> source, Map<String, List<MessageQueue>> dest) {
        dest.clear();
        for (Map.Entry<String, List<MessageQueue>> entry : source.entrySet()) {
            dest.put(entry.getKey(), new ArrayList<MessageQueue>(entry.getValue()));
        }
    }

    private List<MessageQueue> getPrevMqAll() {
        List<MessageQueue> prevMqAll = new ArrayList<MessageQueue>();
        for (List<MessageQueue> queues : messageQueueAllocation.values()) {
            prevMqAll.addAll(queues);
        }
        return prevMqAll;
    }

    @Override
    public String getName() {
        return "STICKY";
    }
}
