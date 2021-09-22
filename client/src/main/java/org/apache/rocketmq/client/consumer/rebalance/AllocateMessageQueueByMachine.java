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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.*;


public class AllocateMessageQueueByMachine implements AllocateMessageQueueStrategy {
    private InternalLogger log;
    private final AllocateMessageQueueStrategy delegator;

    public AllocateMessageQueueByMachine(AllocateMessageQueueStrategy strategy) {
        this.log = ClientLogger.getLog();
        this.delegator = strategy;
    }

    public AllocateMessageQueueByMachine(InternalLogger log, AllocateMessageQueueStrategy strategy) {
        this.log = log;
        this.delegator = strategy;
    }

    /**
     * @param topicRouteData runtime info of topic route data,
     *                       in client,topicRouteData will be update when start, and check every 30s in scheduledExecutorService
     * @param consumerGroup  current consumer group
     * @param currentCID     current consumer id
     * @param mqAll          message queue set in current topic
     * @param cidAll         consumer set in current consumer group
     * @return
     */
    @Override
    public List<MessageQueue> allocate(TopicRouteData topicRouteData, String consumerGroup,
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
            return delegator.allocate(null, consumerGroup, currentCID, mqAll, cidAll);
        }


        String[] split = currentCID.split("@");
        String currentClientIp = split[0];

        List<String> allClientIp = new ArrayList<String>();
        for (String cid : cidAll) {
            String[] cidArray = cid.split("@");
            allClientIp.add(cidArray[0]);
        }

        List<BrokerData> brokerDatas = topicRouteData.getBrokerDatas();

        /**
         * MessageQueue Grouped by br brokerAddr
         */
        Map<String/*brokerAddr ip:port*/, List<MessageQueue>> brokerAddrAndMq = new HashMap<String, List<MessageQueue>>();
        for (MessageQueue mq : mqAll) {
            for (BrokerData brokerData : brokerDatas) {
                if (brokerData.getBrokerName().equals(mq.getBrokerName())) {
                    HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();

                    String mqAddr = brokerAddrs.get(MixAll.MASTER_ID);

                    List<MessageQueue> messageQueues = brokerAddrAndMq.get(mqAddr);
                    if (messageQueues == null) {
                        messageQueues = new ArrayList<MessageQueue>();
                        brokerAddrAndMq.put(mqAddr, messageQueues);
                    }

                    messageQueues.add(mq);
                }
            }
        }

        /**
         * if clientIp same as brokerAddr, find corresponding MessageQueue.
         */
        Map<String/*clientIp*/, List<MessageQueue>> clientIpAndMq = new HashMap<String, List<MessageQueue>>();

        for (String clientIp : allClientIp) {
            for (String brokerIpAndPort : brokerAddrAndMq.keySet()) {
                if (brokerIpAndPort.contains(clientIp)) {
                    List<MessageQueue> messageQueues = brokerAddrAndMq.get(brokerIpAndPort);
                    if (messageQueues != null) {
                        List<MessageQueue> queueList = clientIpAndMq.get(clientIp);
                        if (queueList == null) {
                            clientIpAndMq.put(clientIp, messageQueues);
                        } else {
                            queueList.addAll(messageQueues);
                        }

                    }
                }
            }
        }

        if (clientIpAndMq.size() == 0) {
            return delegator.allocate(topicRouteData, consumerGroup, currentCID, mqAll, cidAll);
        }


        Iterator<Map.Entry<String, List<MessageQueue>>> iterator = brokerAddrAndMq.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<MessageQueue>> entry = iterator.next();

            for (String clientIp : clientIpAndMq.keySet()) {
                if (entry.getKey().contains(clientIp)) {
                    iterator.remove();
                }
            }
        }


        /**
         * remain messageQueues which brokerAddr have no equal clientIp
         */
        List<MessageQueue> mqRemain = null;
        if (brokerAddrAndMq.size() != 0) {
            mqRemain = new ArrayList<MessageQueue>();
            for (List<MessageQueue> list : brokerAddrAndMq.values()) {
                mqRemain.addAll(list);
            }
        }

        double avgNum = mqAll.size() / (cidAll.size() * 1.0);
        //If a clientIp in clientIpAndMq consumes too much mq, it will be divided.
        for (List<MessageQueue> queues : clientIpAndMq.values()) {
            int size = queues.size();
            if (size > avgNum) {
                if (mqRemain == null) {
                    mqRemain = new ArrayList<MessageQueue>();
                }

                Iterator<MessageQueue> queueIterator = queues.iterator();

                double avgRemain = 0.0;
                int remainCidWithoutEqualBrokerAddrNum = cidAll.size() - clientIpAndMq.size();
                if (remainCidWithoutEqualBrokerAddrNum != 0) {
                    avgRemain = mqRemain.size() / (remainCidWithoutEqualBrokerAddrNum * 1.0);
                }
                while (queues.size() > avgNum && ((queues.size() - 1) >= avgNum || queues.size() - Math.floor(avgRemain) > 1)) {
                    mqRemain.add(queueIterator.next());
                    queueIterator.remove();
                    avgRemain = mqRemain.size() / (remainCidWithoutEqualBrokerAddrNum * 1.0);
                }
            }
        }


        List<MessageQueue> messageQueues = clientIpAndMq.get(currentClientIp);
        if (messageQueues == null) {
            if (mqRemain == null || mqRemain.size() == 0) {
                return result;
            } else {
                //Exclude cid that has been assigned mq
                List<String> copy = new ArrayList<String>(cidAll);
                Iterator<String> cidIterator = copy.iterator();
                while (cidIterator.hasNext()) {
                    String cid = cidIterator.next();
                    for (String clientIp : clientIpAndMq.keySet()) {
                        if (cid.contains(clientIp)) {
                            cidIterator.remove();
                        }
                    }

                }

                return delegator.allocate(topicRouteData, consumerGroup, currentCID, mqRemain, copy);
            }
        } else {//If currentClientIp has a corresponding nearby consumer object
            result.addAll(messageQueues);
            if (messageQueues.size() < Math.floor(avgNum) && mqRemain != null && mqRemain.size() != 0) {
                //If the allocated number is less than the average number, allocate some from the remaining mq
                List<MessageQueue> allocated = delegator.allocate(topicRouteData, consumerGroup, currentCID, mqRemain, cidAll);
                result.addAll(allocated);
            }
            return result;
        }
    }

    @Override
    public String getName() {
        return "MACHINE_IP";
    }


}
