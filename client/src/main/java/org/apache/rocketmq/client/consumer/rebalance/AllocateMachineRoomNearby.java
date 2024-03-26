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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * An allocate strategy proxy for based on machine room nearside priority. An actual allocate strategy can be
 * specified.
 *
 * If any consumer is alive in a machine room, the message queue of the broker which is deployed in the same machine
 * should only be allocated to those. Otherwise, those message queues can be shared along all consumers since there are
 * no alive consumer to monopolize them.
 */
public class AllocateMachineRoomNearby extends AbstractAllocateMessageQueueStrategy {

    private final AllocateMessageQueueStrategy allocateMessageQueueStrategy;//actual allocate strategy
    private final MachineRoomResolver machineRoomResolver;

    public AllocateMachineRoomNearby(AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MachineRoomResolver machineRoomResolver) throws NullPointerException {
        checkParams(allocateMessageQueueStrategy, machineRoomResolver);

        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.machineRoomResolver = machineRoomResolver;
    }

    private static void checkParams(AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MachineRoomResolver machineRoomResolver) {
        if (allocateMessageQueueStrategy == null) {
            throw new NullPointerException("allocateMessageQueueStrategy is null");
        }

        if (machineRoomResolver == null) {
            throw new NullPointerException("machineRoomResolver is null");
        }
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return Collections.emptyList();
        }

        return allocateQueues(consumerGroup, currentCID, cidAll, mqAll);
    }

    private Map<String, List<MessageQueue>> groupMqByMachineRoom(List<MessageQueue> mqAll) {
        Map<String/*machine room */, List<MessageQueue>> mr2Mq = new TreeMap<>();
        for (MessageQueue mq : mqAll) {
            String brokerMachineRoom = machineRoomResolver.brokerDeployIn(mq);
            if (StringUtils.isNoneEmpty(brokerMachineRoom)) {
                if (mr2Mq.get(brokerMachineRoom) == null) {
                    mr2Mq.put(brokerMachineRoom, new ArrayList<>());
                }
                mr2Mq.get(brokerMachineRoom).add(mq);
            } else {
                throw new IllegalArgumentException("Machine room is null for mq " + mq);
            }
        }
        return mr2Mq;
    }

    private Map<String, List<String>> groupConsumerByMachineRoom(List<String> cidAll) {
        Map<String/*machine room */, List<String/*clientId*/>> mr2c = new TreeMap<>();
        for (String cid : cidAll) {
            String consumerMachineRoom = machineRoomResolver.consumerDeployIn(cid);
            if (StringUtils.isNoneEmpty(consumerMachineRoom)) {
                if (mr2c.get(consumerMachineRoom) == null) {
                    mr2c.put(consumerMachineRoom, new ArrayList<>());
                }
                mr2c.get(consumerMachineRoom).add(cid);
            } else {
                throw new IllegalArgumentException("Machine room is null for consumer id " + cid);
            }
        }
        return mr2c;
    }

    private List<MessageQueue> allocateQueues(String consumerGroup, String currentCID, List<String> cidAll,
        List<MessageQueue> mqAll) {
        Map<String, List<MessageQueue>> mr2Mq = groupMqByMachineRoom(mqAll);
        Map<String, List<String>> mr2c = groupConsumerByMachineRoom(cidAll);

        List<MessageQueue> result = new ArrayList<>();

        //1.allocate the mq that deploy in the same machine room with the current consumer
        result.addAll(allocateSameRoomQueues(consumerGroup, currentCID, mr2Mq, mr2c));

        //2.allocate the rest mq to each machine room if there are no consumer alive in that machine room
        result.addAll(allocateRestQueues(consumerGroup, currentCID, cidAll, mr2Mq, mr2c));
        return result;
    }

    private List<MessageQueue> allocateSameRoomQueues(String consumerGroup, String currentCID, Map<String, List<MessageQueue>> mr2Mq,
        Map<String, List<String>> mr2c) {
        String currentMachineRoom = machineRoomResolver.consumerDeployIn(currentCID);
        List<MessageQueue> mqInThisMachineRoom = mr2Mq.remove(currentMachineRoom);
        List<String> consumerInThisMachineRoom = mr2c.get(currentMachineRoom);
        if (mqInThisMachineRoom != null && !mqInThisMachineRoom.isEmpty()) {
            return allocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mqInThisMachineRoom, consumerInThisMachineRoom);
        }

        return Collections.emptyList();
    }

    private List<MessageQueue> allocateRestQueues(String consumerGroup, String currentCID, List<String> cidAll,
        Map<String, List<MessageQueue>> mr2Mq, Map<String, List<String>> mr2c) {
        List<MessageQueue> result = new ArrayList<>();

        for (Entry<String, List<MessageQueue>> machineRoomEntry : mr2Mq.entrySet()) {
            if (!mr2c.containsKey(machineRoomEntry.getKey())) {
                // no alive consumer in the corresponding machine room, so all consumers share these queues
                result.addAll(allocateMessageQueueStrategy.allocate(consumerGroup, currentCID, machineRoomEntry.getValue(), cidAll));
            }
        }

        return result;
    }

    @Override
    public String getName() {
        return "MACHINE_ROOM_NEARBY" + "-" + allocateMessageQueueStrategy.getName();
    }

    /**
     * A resolver object to determine which machine room do the message queues or clients are deployed in.
     *
     * AllocateMachineRoomNearby will use the results to group the message queues and clients by machine room.
     *
     * The result returned from the implemented method CANNOT be null.
     */
    public interface MachineRoomResolver {
        String brokerDeployIn(MessageQueue messageQueue);

        String consumerDeployIn(String clientID);
    }
}
