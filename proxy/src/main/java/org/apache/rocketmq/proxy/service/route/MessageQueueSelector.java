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
import com.google.common.math.IntMath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.QueueData;

public class MessageQueueSelector {
    private static final int BROKER_ACTING_QUEUE_ID = -1;

    // multiple queues for brokers with queueId : normal
    private final List<AddressableMessageQueue> queues = new ArrayList<>();
    // one queue for brokers with queueId : -1
    private final List<AddressableMessageQueue> brokerActingQueues = new ArrayList<>();
    private final Map<String, AddressableMessageQueue> brokerNameQueueMap = new ConcurrentHashMap<>();
    private final AtomicInteger queueIndex;
    private final AtomicInteger brokerIndex;

    public MessageQueueSelector(TopicRouteWrapper topicRouteWrapper, boolean read) {
        if (read) {
            this.queues.addAll(buildRead(topicRouteWrapper));
        } else {
            this.queues.addAll(buildWrite(topicRouteWrapper));
        }
        buildBrokerActingQueues(topicRouteWrapper.getTopicName(), this.queues);
        Random random = new Random();
        this.queueIndex = new AtomicInteger(random.nextInt());
        this.brokerIndex = new AtomicInteger(random.nextInt());
    }

    private static List<AddressableMessageQueue> buildRead(TopicRouteWrapper topicRoute) {
        Set<AddressableMessageQueue> queueSet = new HashSet<>();
        List<QueueData> qds = topicRoute.getQueueDatas();
        if (qds == null) {
            return new ArrayList<>();
        }

        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                String brokerAddr = topicRoute.getMasterAddrPrefer(qd.getBrokerName());
                if (brokerAddr == null) {
                    continue;
                }

                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    AddressableMessageQueue mq = new AddressableMessageQueue(
                        new MessageQueue(topicRoute.getTopicName(), qd.getBrokerName(), i),
                        brokerAddr);
                    queueSet.add(mq);
                }
            }
        }

        return queueSet.stream().sorted().collect(Collectors.toList());
    }

    private static List<AddressableMessageQueue> buildWrite(TopicRouteWrapper topicRoute) {
        Set<AddressableMessageQueue> queueSet = new HashSet<>();
        // order topic route.
        if (StringUtils.isNotBlank(topicRoute.getOrderTopicConf())) {
            String[] brokers = topicRoute.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                String brokerName = item[0];
                String brokerAddr = topicRoute.getMasterAddr(brokerName);
                if (brokerAddr == null) {
                    continue;
                }

                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    AddressableMessageQueue mq = new AddressableMessageQueue(
                        new MessageQueue(topicRoute.getTopicName(), brokerName, i),
                        brokerAddr);
                    queueSet.add(mq);
                }
            }
        } else {
            List<QueueData> qds = topicRoute.getQueueDatas();
            if (qds == null) {
                return new ArrayList<>();
            }

            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    String brokerAddr = topicRoute.getMasterAddr(qd.getBrokerName());
                    if (brokerAddr == null) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        AddressableMessageQueue mq = new AddressableMessageQueue(
                            new MessageQueue(topicRoute.getTopicName(), qd.getBrokerName(), i),
                            brokerAddr);
                        queueSet.add(mq);
                    }
                }
            }
        }

        return queueSet.stream().sorted().collect(Collectors.toList());
    }

    private void buildBrokerActingQueues(String topic, List<AddressableMessageQueue> normalQueues) {
        for (AddressableMessageQueue mq : normalQueues) {
            AddressableMessageQueue brokerActingQueue = new AddressableMessageQueue(
                new MessageQueue(topic, mq.getMessageQueue().getBrokerName(), BROKER_ACTING_QUEUE_ID),
                mq.getBrokerAddr());

            if (!brokerActingQueues.contains(brokerActingQueue)) {
                brokerActingQueues.add(brokerActingQueue);
                brokerNameQueueMap.put(brokerActingQueue.getBrokerName(), brokerActingQueue);
            }
        }

        Collections.sort(brokerActingQueues);
    }

    public AddressableMessageQueue getQueueByBrokerName(String brokerName) {
        return this.brokerNameQueueMap.get(brokerName);
    }

    public AddressableMessageQueue selectOne(boolean onlyBroker) {
        int nextIndex = onlyBroker ? brokerIndex.getAndIncrement() : queueIndex.getAndIncrement();
        return selectOneByIndex(nextIndex, onlyBroker);
    }

    public AddressableMessageQueue selectNextOne(AddressableMessageQueue last) {
        boolean onlyBroker = last.getQueueId() < 0;
        AddressableMessageQueue newOne = last;
        int count = onlyBroker ? brokerActingQueues.size() : queues.size();

        for (int i = 0; i < count; i++) {
            newOne = selectOne(onlyBroker);
            if (!newOne.getBrokerName().equals(last.getBrokerName()) || newOne.getQueueId() != last.getQueueId()) {
                break;
            }
        }
        return newOne;
    }

    public AddressableMessageQueue selectOneByIndex(int index, boolean onlyBroker) {
        if (onlyBroker) {
            if (brokerActingQueues.isEmpty()) {
                return null;
            }
            return brokerActingQueues.get(IntMath.mod(index, brokerActingQueues.size()));
        }

        if (queues.isEmpty()) {
            return null;
        }
        return queues.get(IntMath.mod(index, queues.size()));
    }

    public List<AddressableMessageQueue> getQueues() {
        return queues;
    }

    public List<AddressableMessageQueue> getBrokerActingQueues() {
        return brokerActingQueues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageQueueSelector)) {
            return false;
        }
        MessageQueueSelector queue = (MessageQueueSelector) o;
        return Objects.equals(queues, queue.queues) &&
            Objects.equals(brokerActingQueues, queue.brokerActingQueues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queues, brokerActingQueues);
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
