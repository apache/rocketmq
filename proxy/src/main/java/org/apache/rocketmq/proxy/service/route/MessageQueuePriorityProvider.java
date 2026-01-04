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

import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

@FunctionalInterface
public interface MessageQueuePriorityProvider<Q extends MessageQueue> {
    /**
     * smaller value is higher priority
     * */
    int priorityOf(Q q);

    static <Q extends MessageQueue> List<List<Q>> buildPriorityGroups(List<Q> queues, MessageQueuePriorityProvider<Q> provider) {
        if (queues == null || queues.isEmpty()) {
            return java.util.Collections.emptyList();
        }

        java.util.Map<Integer, List<Q>> buckets = new java.util.TreeMap<>();
        for (Q q : queues) {
            int p = provider.priorityOf(q);
            buckets.computeIfAbsent(p, k -> new java.util.ArrayList<>()).add(q);
        }
        return new java.util.ArrayList<>(buckets.values());
    }
}
