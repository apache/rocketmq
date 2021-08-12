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

package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocateMessageQueueByMachine implements AllocateMessageQueueStrategy {
    private InternalLogger log;
    private AllocateMessageQueueStrategy delegtor;

    public AllocateMessageQueueByMachine(AllocateMessageQueueStrategy strategy) {
        this.log = ClientLogger.getLog();
        this.delegtor = strategy;
    }

    public AllocateMessageQueueByMachine(InternalLogger log, AllocateMessageQueueStrategy strategy) {
        this.log = log;
        this.delegtor = strategy;
    }

    /**
     *
     * @param topicRouteData runtime info of topic route data,
     *                       in client,topicRouteData will be update when start, and check every 30s in scheduledExecutorService
     * @param consumerGroup  current consumer group
     * @param currentCID     current consumer id
     * @param mqAll          message queue set in current topic
     * @param cidAll         consumer set in current consumer group
     * @return
     */
    @Override
    public List<MessageQueue> allocate(Map<String, TopicRouteData> topicRouteData, String consumerGroup,
                                       String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
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

        if (topicRouteData == null) {
            return delegtor.allocate(null, consumerGroup, currentCID, mqAll, cidAll);
        }


        String[] split = currentCID.split("@");
        String clientIp = split[0];


        for (MessageQueue mq : mqAll) {
            String topic = mq.getTopic();
            TopicRouteData routeData = topicRouteData.get(topic);
            List<BrokerData> brokerDatas = routeData.getBrokerDatas();

            for (BrokerData brokerData : brokerDatas) {
                if (brokerData.getBrokerName().equals(mq.getBrokerName())) {
                    HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();

                    for (Long brokerId : brokerAddrs.keySet()) {
                        String brokerAddr = brokerAddrs.get(brokerId);
                        if (brokerAddr.contains(clientIp)) {
                            result.add(mq);
                        }
                    }
                }
            }
        }

        if (result.size() == 0) {
            return delegtor.allocate(topicRouteData, consumerGroup, currentCID, mqAll, cidAll);
        } else {
            return result;
        }
    }

    @Override
    public String getName() {
        return "MACHINE";
    }


}
