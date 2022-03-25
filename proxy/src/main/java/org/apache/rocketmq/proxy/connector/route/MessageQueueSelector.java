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
package org.apache.rocketmq.proxy.connector.route;

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
import org.apache.rocketmq.common.protocol.route.QueueData;

public class MessageQueueSelector {
    private static final int BROKER_ACTING_QUEUE_ID = -1;

    // multiple queues for one broker, with queueId : normal
    private final List<SelectableMessageQueue> queues = new ArrayList<>();
    // one queue for one broker, with queueId : -1
    private final List<SelectableMessageQueue> brokerActingQueues = new ArrayList<>();
    private final Map<String, SelectableMessageQueue> brokerNameQueueMap = new ConcurrentHashMap<>();
    private final AtomicInteger queueIndex;
    private final AtomicInteger brokerIndex;

    public MessageQueueSelector(TopicRouteWrapper topicRouteWrapper, boolean read) {
        if (read) {
            this.queues.addAll(buildRead(topicRouteWrapper));
        } else {
            this.queues.addAll(buildWrite(topicRouteWrapper));
        }
        buildBrokerActingQueues(topicRouteWrapper.getTopicName(), this.queues);

        this.queueIndex = new AtomicInteger(Math.abs(new Random().nextInt()));
        this.brokerIndex = new AtomicInteger(Math.abs(new Random().nextInt()));
    }

    private static List<SelectableMessageQueue> buildRead(TopicRouteWrapper topicRoute) {
        Set<SelectableMessageQueue> queueSet = new HashSet<>();
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
                    SelectableMessageQueue mq = new SelectableMessageQueue(
                        new MessageQueue(topicRoute.getTopicName(), qd.getBrokerName(), i),
                        brokerAddr);
                    queueSet.add(mq);
                }
            }
        }

        return queueSet.stream().sorted().collect(Collectors.toList());
    }

    private static List<SelectableMessageQueue> buildWrite(TopicRouteWrapper topicRoute) {
        Set<SelectableMessageQueue> queueSet = new HashSet<>();
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
                    SelectableMessageQueue mq = new SelectableMessageQueue(
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
                        SelectableMessageQueue mq = new SelectableMessageQueue(
                            new MessageQueue(topicRoute.getTopicName(), qd.getBrokerName(), i),
                            brokerAddr);
                        queueSet.add(mq);
                    }
                }
            }
        }

        return queueSet.stream().sorted().collect(Collectors.toList());
    }

    private void buildBrokerActingQueues(String topic, List<SelectableMessageQueue> normalQueues) {
        for (SelectableMessageQueue mq : normalQueues) {
            SelectableMessageQueue brokerActingQueue = new SelectableMessageQueue(
                new MessageQueue(topic, mq.getMessageQueue().getBrokerName(), BROKER_ACTING_QUEUE_ID),
                mq.getBrokerAddr());

            if (!brokerActingQueues.contains(brokerActingQueue)) {
                brokerActingQueues.add(brokerActingQueue);
                brokerNameQueueMap.put(brokerActingQueue.getBrokerName(), brokerActingQueue);
            }
        }

        Collections.sort(brokerActingQueues);
    }

    public final SelectableMessageQueue getQueueByBrokerName(String brokerName) {
        return this.brokerNameQueueMap.get(brokerName);
    }

    public final SelectableMessageQueue selectOne(boolean onlyBroker) {
        int nextIndex = onlyBroker ? brokerIndex.getAndIncrement() : queueIndex.getAndIncrement();
        return selectOneByIndex(nextIndex, onlyBroker);
    }

    public final SelectableMessageQueue selectOne(String brokerName, int queueId) {
        for (SelectableMessageQueue targetMessageQueue : queues) {
            String queueBrokerName = targetMessageQueue.getBrokerName();
            if (queueBrokerName.equals(brokerName) && targetMessageQueue.getQueueId() == queueId) {
                return targetMessageQueue;
            }
        }
        return null;
    }

    public final SelectableMessageQueue selectOneByIndex(int index, boolean onlyBroker) {
        if (onlyBroker) {
            if (brokerActingQueues.isEmpty()) {
                return null;
            }
            return brokerActingQueues.get(Math.abs(index) % brokerActingQueues.size());
        }

        if (queues.isEmpty()) {
            return null;
        }
        return queues.get(Math.abs(index) % queues.size());
    }

    // find next same type(but different) queue with last(normal queue or broker acting queue).
    public final SelectableMessageQueue selectNextQueue(SelectableMessageQueue last) {
        boolean onlyBroker = last.getQueueId() < 0;
        SelectableMessageQueue newOne = last;
        int count = onlyBroker ? brokerActingQueues.size() : queues.size();

        for (int i = 0; i < count; i++) {
            newOne = selectOne(onlyBroker);
            if (!newOne.getBrokerName().equals(last.getBrokerName()) || newOne.getQueueId() != last.getQueueId()) {
                break;
            }
        }

        return newOne;
    }

    public List<SelectableMessageQueue> getQueues() {
        return queues;
    }

    public List<SelectableMessageQueue> getBrokerActingQueues() {
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
        return "SelectableMessageQueue{" + "queues=" + queues +
            ", brokers=" + brokerActingQueues +
            ", queueIndex=" + queueIndex +
            ", brokerIndex=" + brokerIndex +
            '}';
    }
}
