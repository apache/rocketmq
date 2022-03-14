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
package org.apache.rocketmq.proxy.client.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;

public class SelectableMessageQueue {

    // queueId : normal
    private final List<AddressableMessageQueue> queues;
    // queueId : -1
    private final List<AddressableMessageQueue> brokers;
    private final Map<String, AddressableMessageQueue> brokerNameMap;
    private final AtomicInteger queueIndex;
    private final AtomicInteger brokerIndex;

    public SelectableMessageQueue(TopicRouteWrapper topicRouteWrapper, boolean read) {
        this.queues = new ArrayList<>();
        this.brokers = new ArrayList<>();
        this.brokerNameMap = new HashMap<>();
        if (read) {
            this.queues.addAll(buildRead(topicRouteWrapper));
        } else {
            this.queues.addAll(buildWrite(topicRouteWrapper));
        }
        buildBroker(topicRouteWrapper.getTopicName(), this.queues);

        this.queueIndex = new AtomicInteger(Math.abs(new Random().nextInt()));
        this.brokerIndex = new AtomicInteger(Math.abs(new Random().nextInt()));
    }

    private static List<AddressableMessageQueue> buildRead(
        TopicRouteWrapper topicRoute) {
        List<AddressableMessageQueue> queues = new ArrayList<>();
        List<QueueData> qds = topicRoute.getQueueDatas();
        if (qds == null) {
            return queues;
        }
        Collections.sort(qds);
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
                    if (!queues.contains(mq)) {
                        queues.add(mq);
                    }
                }
            }
        }

        Collections.sort(queues);
        return queues;
    }

    private static List<AddressableMessageQueue> buildWrite(
        TopicRouteWrapper topicRoute) {
        List<AddressableMessageQueue> queues = new ArrayList<>();
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
                    if (!queues.contains(mq)) {
                        queues.add(mq);
                    }
                }
            }
        } else {
            List<QueueData> qds = topicRoute.getQueueDatas();
            if (qds == null) {
                return queues;
            }
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    String brokerName = qd.getBrokerName();
                    String brokerAddr = topicRoute.getMasterAddr(brokerName);
                    if (brokerAddr == null) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        AddressableMessageQueue mq = new AddressableMessageQueue(
                            new MessageQueue(topicRoute.getTopicName(), brokerName, i),
                            brokerAddr);
                        if (!queues.contains(mq)) {
                            queues.add(mq);
                        }
                    }
                }
            }
        }

        Collections.sort(queues);
        return queues;
    }

    private void buildBroker(String topic, List<AddressableMessageQueue> queues) {
        for (AddressableMessageQueue messageQueue : queues) {
            AddressableMessageQueue mb = new AddressableMessageQueue(
                new MessageQueue(topic, messageQueue.getMessageQueue().getBrokerName(), -1),
                messageQueue.getBrokerAddr());
            if (!brokers.contains(mb)) {
                brokers.add(mb);
                brokerNameMap.put(mb.getBrokerName(), mb);
            }
        }

        Collections.sort(brokers);
    }

    public final AddressableMessageQueue getBrokerByName(String brokerName) {
        return this.brokerNameMap.get(brokerName);
    }

    public final AddressableMessageQueue selectOne(boolean onlyBroker) {
        return selectOneByIndex(onlyBroker ? brokerIndex.getAndIncrement() : queueIndex.getAndIncrement(), onlyBroker);
    }

    public final AddressableMessageQueue selectOne(String brokerName, int queueId) {
        for (AddressableMessageQueue addressableMessageQueue : queues) {
            String queueBrokerName = addressableMessageQueue.getBrokerName();
            if (queueBrokerName.equals(brokerName) && addressableMessageQueue.getQueueId() == queueId) {
                return addressableMessageQueue;
            }
        }
        return null;
    }

    public final AddressableMessageQueue selectOneByIndex(int index, boolean onlyBroker) {
        if (onlyBroker) {
            if (brokers.isEmpty()) {
                return null;
            }
            return brokers.get(Math.abs(index) % brokers.size());
        }
        if (queues.isEmpty()) {
            return null;
        }
        return queues.get(Math.abs(index) % queues.size());
    }

    public final AddressableMessageQueue selectNextOne(
        AddressableMessageQueue last) {
        boolean onlyBroker = last.getQueueId() < 0;
        AddressableMessageQueue newOne = last;
        int count = onlyBroker ? brokers.size() : queues.size();

        for (int i = 0; i < count; i++) {
            newOne = selectOne(onlyBroker);
            if (!newOne.getBrokerName().equals(last.getBrokerName()) || newOne.getQueueId() != last.getQueueId()) {
                break;
            }
        }

        return newOne;
    }

    public final AddressableMessageQueue selectNextBrokerOne(
        AddressableMessageQueue last) {
        boolean onlyBroker = last.getQueueId() < 0;
        AddressableMessageQueue newOne = last;
        int count = onlyBroker ? brokers.size() : queues.size();

        for (int i = 0; i < count; i++) {
            newOne = selectOne(onlyBroker);
            if (!newOne.getBrokerName().equals(last.getBrokerName())) {
                break;
            }
        }

        return newOne;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SelectableMessageQueue)) {
            return false;
        }
        SelectableMessageQueue queue = (SelectableMessageQueue) o;
        return Objects.equals(queues, queue.queues) &&
            Objects.equals(brokers, queue.brokers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queues, brokers);
    }

    @Override
    public String toString() {
        return "SelectableMessageQueue{" + "queues=" + queues +
            ", brokers=" + brokers +
            ", queueIndex=" + queueIndex +
            ", brokerIndex=" + brokerIndex +
            '}';
    }
}
