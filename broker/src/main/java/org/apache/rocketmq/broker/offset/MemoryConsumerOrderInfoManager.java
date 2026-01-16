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

package org.apache.rocketmq.broker.offset;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.pop.orderly.QueueLevelConsumerManager;

/**
 * Memory-based Consumer Order Information Manager for Lite Topics
 * Trade-off considerations::
 * 1. Lite Topics are primarily used for lightweight consumption where
 *    strict ordering requirements are relatively low
 * 2. Considering compatibility with traditional PushConsumer,
 *    a certain degree of ordering control failure is acceptable
 * 3. Avoiding I/O overhead from persistence operations
 * <p>
 * We may make structural adjustments and optimizations to reduce overhead and memory footprint.
 */
public class MemoryConsumerOrderInfoManager extends QueueLevelConsumerManager {

    public MemoryConsumerOrderInfoManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    protected void updateLockFreeTimestamp(String topic, String group, int queueId, OrderInfo orderInfo) {
        if (this.getConsumerOrderInfoLockManager() != null) {
            // use max lock free time to prevent unexpected blocking
            this.getConsumerOrderInfoLockManager().updateLockFreeTimestamp(
                topic, group, queueId, orderInfo.getMaxLockFreeTimestamp());
        }
    }

    @Override
    public void persist() {
        // MemoryConsumerOrderInfoManager persist, do nothing.
    }
}
