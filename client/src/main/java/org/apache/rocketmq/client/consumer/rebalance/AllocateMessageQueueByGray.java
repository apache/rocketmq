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

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.GrayConstants;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * AllocateMessageQueueByGray
 * Allocate the message queue based on the gray strategy.
 */
public class AllocateMessageQueueByGray extends AllocateMessageQueueAveragely {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return Collections.emptyList();
        }
        // Retried message queues do not participate in gray-scale load balancing, following the default RocketMQ strategy instead.
        // Retried messages are re-sent by the client back to the broker, not through the default send logic. They are written to the retry topic of the group,
        // and we do not know which queue they will end up in, hence gray-scale load balancing cannot be applied
        if (mqAll.stream().anyMatch(mq -> mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX))) {
            return super.allocate(consumerGroup, currentCID, mqAll, cidAll);
        }
        // If there is no gray-scale service available, then allocation is performed according to the average distribution strategy.
        if (!hasGrayTag(cidAll)) {
            List<MessageQueue> allocate = super.allocate(consumerGroup, currentCID, mqAll, cidAll);
            if (log.isInfoEnabled()) {
                log.info("topic:{} reBalance, no gray release client, allocate {} message queue by average strategy.\n" +
                                "current cid:{}\n" +
                                "result:\n{}",
                        mqAll.get(0).getTopic(),
                        allocate.size(),
                        currentCID,
                        allocate.stream()
                                .collect(Collectors.groupingBy(MessageQueue::getBrokerName))
                                .entrySet().stream()
                                .map(e -> e.getKey() + ": " + e.getValue().stream()
                                        .map(m -> String.valueOf(m.getQueueId()))
                                        .collect(Collectors.joining(", ")))
                                .collect(Collectors.joining("\n")));
            }
            return allocate;
        }
        List<String> grayCids = getGrayCids(cidAll);
        List<String> normalCids = getNormalCids(cidAll);
        List<MessageQueue> grayQueues = splitMessageQueues(mqAll).getObject1();
        List<MessageQueue> normalQueues = splitMessageQueues(mqAll).getObject2();
        sortLists(grayCids, normalCids, grayQueues, normalQueues);

        List<MessageQueue> result;
        if (grayCids.contains(currentCID)) {
            result = super.allocate(consumerGroup, currentCID, grayQueues, grayCids);
        } else {
            result = super.allocate(consumerGroup, currentCID, normalQueues, normalCids);
        }
        if (log.isDebugEnabled()) {
            log.info("topic:{} reBalance,allocate, has gary release client, allocate {} message queue by gray release strategy.\n" +
                            "current cid:{}\n" +
                            "result:\n{}",
                    mqAll.get(0).getTopic(),
                    result.size(),
                    currentCID,
                    result.stream()
                            .collect(Collectors.groupingBy(MessageQueue::getBrokerName))
                            .entrySet().stream()
                            .map(e -> e.getKey() + ": " + e.getValue().stream()
                                    .map(m -> String.valueOf(m.getQueueId()))
                                    .collect(Collectors.joining(", ")))
                            .collect(Collectors.joining("\n")));
        }
        return result;
    }

    @Override
    public String getName() {
        return "GRAY";
    }


    public List<MessageQueue> allocate4Pop(String consumerGroup, final String clientId, List<MessageQueue> mqAll, List<String> cidAll, int popShareQueueNum) {
        if (!check(consumerGroup, clientId, mqAll, cidAll)) {
            return Collections.emptyList();
        }
        // Principle: Gray-scale clients should split and consume from gray-scale queues, while other clients are allocated non-gray-scale queues for consumption.
        List<String> grayCids = getGrayCids(cidAll);
        List<String> normalCids = getNormalCids(cidAll);
        List<MessageQueue> grayQueues = splitMessageQueues(mqAll).getObject1();
        List<MessageQueue> normalQueues = splitMessageQueues(mqAll).getObject2();
        sortLists(grayCids, normalCids, grayQueues, normalQueues);
        if (grayCids.contains(clientId)) {
            mqAll = grayQueues;
            cidAll = grayCids;
        } else {
            mqAll = normalQueues;
            cidAll = normalCids;
        }

        if (log.isDebugEnabled()) {
            log.warn("AllocateMessageQueueByGray allocate4Pop handle clientId:{},cidAll:{},mqAll:{}", clientId, cidAll, mqAll);
        }

        List<MessageQueue> allocateResult;
        if (popShareQueueNum <= 0 || popShareQueueNum >= cidAll.size() - 1) {
            //each client pop all messagequeue
            allocateResult = new ArrayList<>(mqAll.size());
            for (MessageQueue mq : mqAll) {
                //must create new MessageQueue in case of change cache in AssignmentManager
                MessageQueue newMq = new MessageQueue(mq.getTopic(), mq.getBrokerName(), mq.getQueueId());
                allocateResult.add(newMq);
            }

        } else {
            if (cidAll.size() <= mqAll.size()) {
                //consumer working in pop mode could share the MessageQueues assigned to the N (N = popWorkGroupSize) consumer following it in the cid list
                allocateResult = super.allocate(consumerGroup, clientId, mqAll, cidAll);
                int index = cidAll.indexOf(clientId);
                if (index >= 0) {
                    for (int i = 1; i <= popShareQueueNum; i++) {
                        index++;
                        index = index % cidAll.size();
                        List<MessageQueue> tmp = super.allocate(consumerGroup, cidAll.get(index), mqAll, cidAll);
                        allocateResult.addAll(tmp);
                    }
                }
            } else {
                //make sure each cid is assigned
                int index = cidAll.indexOf(clientId);
                allocateResult = Collections.singletonList(mqAll.get(index % mqAll.size()));
            }
        }


        if (log.isDebugEnabled()) {
            log.info("topic:{} reBalance,allocate4Pop, has gray release client, allocate {} message queue by gray release strategy.\n" +
                            "current cid:{}\n" +
                            "result:\n{}",
                    mqAll.get(0).getTopic(),
                    allocateResult.size(),
                    clientId,
                    allocateResult.stream()
                            .collect(Collectors.groupingBy(MessageQueue::getBrokerName))
                            .entrySet().stream()
                            .map(e -> e.getKey() + ": " + e.getValue().stream()
                                    .map(m -> String.valueOf(m.getQueueId()))
                                    .collect(Collectors.joining(", ")))
                            .collect(Collectors.joining("\n")));
        }

        return allocateResult;
    }


    /**
     * Splits the given list of MessageQueues into gray and normal queues.
     * Gray queues are the first queue of each broker group, while normal queues are the rest.
     *
     * @param source the list of MessageQueues to be split
     * @return a pair where the left element is the list of gray queues and the right element is the list of normal queues
     */
    public static Pair<List<MessageQueue>, List<MessageQueue>> splitMessageQueues(List<MessageQueue> source) {
        if (CollectionUtils.isEmpty(source)) {
            return new Pair<>(Collections.emptyList(), Collections.emptyList());
        }
        Map<String, List<MessageQueue>> brokerGroupQueues = source.stream()
                .collect(Collectors.groupingBy(MessageQueue::getBrokerName, Collectors.toList()));
        List<MessageQueue> grayQueues = new ArrayList<>();
        List<MessageQueue> normalQueues = new ArrayList<>();
        brokerGroupQueues.forEach((brokerName, queues) -> {
            Collections.sort(queues);
            // last queue is gray
            grayQueues.add(queues.get(queues.size() - 1));
            // Rest are normal
            normalQueues.addAll(queues.subList(0, queues.size() - 1));
        });

        return new Pair<>(grayQueues, normalQueues);
    }

    public static boolean hasGrayTag(List<String> clientIds) {
        return clientIds.stream().anyMatch(AllocateMessageQueueByGray::isGrayTag);
    }

    public static boolean isGrayTag(String clientId) {
        return clientId.contains("@" + GrayConstants.GARY_TAG);
    }

    public static List<String> getGrayCids(List<String> clientIds) {
        return clientIds.stream().filter(AllocateMessageQueueByGray::isGrayTag).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    public static List<String> getNormalCids(List<String> clientIds) {
        return clientIds.stream().filter(cid -> !isGrayTag(cid)).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    /**
     * sort
     *
     * @param grayCids
     * @param normalCids
     * @param grayQueues
     * @param normalQueues
     */
    private void sortLists(List<String> grayCids, List<String> normalCids, List<MessageQueue> grayQueues, List<MessageQueue> normalQueues) {
        Collections.sort(grayCids);
        Collections.sort(normalCids);
        Collections.sort(normalQueues);
        Collections.sort(grayQueues);
    }
}
