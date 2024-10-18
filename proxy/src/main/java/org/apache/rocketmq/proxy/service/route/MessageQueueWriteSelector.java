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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.math.IntMath;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.latency.MQFaultStrategy;

public class MessageQueueWriteSelector extends MessageQueueSelector {
    private final ThreadLocalIndex brokerIndex;
    private final ThreadLocalIndex queueIndex;

    public MessageQueueWriteSelector(TopicRouteWrapper topicRouteWrapper, MQFaultStrategy mqFaultStrategy) {
        super(topicRouteWrapper, mqFaultStrategy, false);
        brokerIndex = new ThreadLocalIndex();
        queueIndex = new ThreadLocalIndex();
    }

    @Override
    public AddressableMessageQueue selectOne(boolean onlyBroker) {
        int nextIndex = onlyBroker ? brokerIndex.incrementAndGet() : queueIndex.incrementAndGet();
        return selectOneByIndex(nextIndex, onlyBroker);
    }

    public AddressableMessageQueue selectOneByPipeline(boolean onlyBroker) {
        if (mqFaultStrategy != null && mqFaultStrategy.isSendLatencyFaultEnable()) {
            List<AddressableMessageQueue> addressableMessageQueueList = onlyBroker ? brokerActingQueues : queues;
            ThreadLocalIndex index = onlyBroker ? brokerIndex : queueIndex;

            AddressableMessageQueue addressableMessageQueue;

            // use available and reachable filters.
            addressableMessageQueue = selectOneMessageQueue(addressableMessageQueueList, index,
                mqFaultStrategy.getAvailableFilter(), mqFaultStrategy.getReachableFilter());
            if (addressableMessageQueue != null) {
                return addressableMessageQueue;
            }

            // use available filter.
            addressableMessageQueue = selectOneMessageQueue(addressableMessageQueueList, index,
                mqFaultStrategy.getAvailableFilter());
            if (addressableMessageQueue != null) {
                return addressableMessageQueue;
            }

            // no available filter, then use reachable filter.
            addressableMessageQueue = selectOneMessageQueue(addressableMessageQueueList, index,
                mqFaultStrategy.getReachableFilter());
            if (addressableMessageQueue != null) {
                return addressableMessageQueue;
            }
        }

        // SendLatency is not enabled, or no queue is selected, then select by index.
        return selectOne(onlyBroker);
    }

    private AddressableMessageQueue selectOneMessageQueue(List<AddressableMessageQueue> addressableMessageQueueList,
        ThreadLocalIndex index, TopicPublishInfo.QueueFilter... filter) {
        if (addressableMessageQueueList == null || addressableMessageQueueList.isEmpty()) {
            return null;
        }
        if (filter != null) {
            // pre-check
            for (TopicPublishInfo.QueueFilter f : filter) {
                Preconditions.checkNotNull(f);
            }
            int size = addressableMessageQueueList.size();
            for (int i = 0; i < size; i++) {
                AddressableMessageQueue amq = addressableMessageQueueList.get(IntMath.mod(index.incrementAndGet(), size));
                boolean filterResult = true;
                for (TopicPublishInfo.QueueFilter f : filter) {
                    filterResult = f.filter(amq.getMessageQueue());
                    if (!filterResult) {
                        break;
                    }
                }
                if (filterResult) {
                    return amq;
                }
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("queues", queues)
            .add("brokerActingQueues", brokerActingQueues)
            .add("brokerNameQueueMap", brokerNameQueueMap)
            .add("queueIndex", queueIndex)
            .add("brokerIndex", brokerIndex)
            .toString();
    }
}
