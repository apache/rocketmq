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

package org.apache.rocketmq.proxy.service.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * A functional interface for providing priority values for message queues.
 * This interface allows custom priority determination logic to be applied to message queues,
 * enabling queue selection and routing based on priority levels.
 * <p>
 * The priority value follows the convention that smaller numeric values indicate higher priority.
 * For example, priority 0 is higher than priority 1.
 * </p>
 *
 * @param <Q> the type of message queue, must extend {@link MessageQueue}
 */
@FunctionalInterface
public interface MessageQueuePriorityProvider<Q extends MessageQueue> {

    /**
     * Determines the priority value of the given message queue.
     * <p>
     * Smaller values indicate higher priority. For example:
     * <ul>
     *   <li>Priority 0: Highest priority</li>
     *   <li>Priority 1: Medium priority</li>
     *   <li>Priority 2: Lower priority</li>
     * </ul>
     * </p>
     *
     * @param q the message queue to evaluate
     * @return the priority value, where smaller values indicate higher priority
     */
    int priorityOf(Q q);

    /**
     * Groups message queues by their priority levels and returns them in priority order.
     * <p>
     * This static utility method takes a list of message queues and a priority provider,
     * then organizes the queues into groups based on their priority values.
     * The returned list is ordered from highest priority to lowest priority.
     * </p>
     *
     * @param <Q>      the type of message queue, must extend {@link MessageQueue}
     * @param queues   the list of message queues to group by priority, can be null or empty
     * @param provider the priority provider to determine the priority of each queue
     * @return a list of lists, where each inner list contains queues of the same priority level,
     *         ordered from highest priority (smallest value) to lowest priority (largest value).
     *         Returns an empty list if the input queues are null or empty.
     */
    static <Q extends MessageQueue> List<List<Q>> buildPriorityGroups(List<Q> queues, MessageQueuePriorityProvider<Q> provider) {
        if (queues == null || queues.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Integer, List<Q>> buckets = new TreeMap<>();
        for (Q q : queues) {
            int p = provider.priorityOf(q);
            buckets.computeIfAbsent(p, k -> new ArrayList<>()).add(q);
        }
        return new ArrayList<>(buckets.values());
    }
}
