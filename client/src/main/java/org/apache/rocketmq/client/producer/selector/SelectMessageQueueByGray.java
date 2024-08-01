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
package org.apache.rocketmq.client.producer.selector;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * SelectMessageQueueByGray
 */
public class SelectMessageQueueByGray implements MessageQueueSelector {

    private final List<MessageQueue> grayQueuesCache = new CopyOnWriteArrayList<>();
    private final List<MessageQueue> normalQueuesCache = new CopyOnWriteArrayList<>();
    private List<MessageQueue> cachedMqs = new CopyOnWriteArrayList<>();
    private final Random random = new Random(System.currentTimeMillis());
    private double grayQueueRatio;

    public SelectMessageQueueByGray() {
        this(0.1);
    }

    public SelectMessageQueueByGray(double grayQueueRatio) {
        this.grayQueueRatio = grayQueueRatio;
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        if (CollectionUtils.isEmpty(mqs)) {
            throw new IllegalArgumentException("MessageQueue list cannot be empty");
        }
        /**
         * Because the MessageQueue class overrides the equals() and hashCode() methods,
         * we can cleverly compare whether the input mqs is equal, caching a cachedMqs.
         * This significantly reduces the performance overhead of selecting a MessageQueue each time under the gray partitioning strategy.
         */
        if (!mqs.equals(cachedMqs)) {
            synchronized (this) {
                if (!mqs.equals(cachedMqs)) {
                    refreshCache(mqs);
                    cachedMqs = mqs;
                }
            }
        }
        // Decide whether to use gray or normal queues based on some condition
        List<MessageQueue> selectedQueues = useGrayQueues(arg instanceof Boolean ? (Boolean) arg : false)
                ? grayQueuesCache
                : normalQueuesCache;
        // Select a queue randomly from the selected list
        return selectedQueues.get(random.nextInt(selectedQueues.size()));
    }


    private boolean useGrayQueues(boolean arg) {
        // Implement your logic here to decide whether to use gray queues  clientConfig#grayTag
        // For example, you might check an argument passed in 'arg' to determine this
        return arg && grayQueueRatio > 0;
    }

    private void refreshCache(List<MessageQueue> mqs) {
        // Clear caches
        grayQueuesCache.clear();
        normalQueuesCache.clear();
        // Ensure grayQueueRatio is within the valid range [0, 1]
        double validGrayQueueRatio = Math.max(0, Math.min(1, grayQueueRatio));
        // Group by broker name
        Map<String, List<MessageQueue>> brokerGroups = mqs.stream()
                .collect(Collectors.groupingBy(MessageQueue::getBrokerName));

        // Determine gray queues and normal queues
        brokerGroups.forEach((brokerName, queues) -> {
            // Sort queues before splitting
            Collections.sort(queues);
            // Calculate the number of gray queues
            int totalQueues = queues.size();
            int numGrayQueues = (int) Math.ceil(totalQueues * validGrayQueueRatio);
            // Determine gray and normal queues
            if (numGrayQueues > 0) {
                // Add the last `numGrayQueues` queues to grayQueuesCache
                grayQueuesCache.addAll(queues.subList(totalQueues - numGrayQueues, totalQueues));
                // Add the remaining queues to normalQueuesCache
                normalQueuesCache.addAll(queues.subList(0, totalQueues - numGrayQueues));
            } else {
                // All queues are normal if no gray queues are designated
                normalQueuesCache.addAll(queues);
            }
        });
    }
}
