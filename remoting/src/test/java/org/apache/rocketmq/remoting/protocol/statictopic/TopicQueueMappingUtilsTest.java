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

package org.apache.rocketmq.remoting.protocol.statictopic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.rocketmq.common.TopicConfig;
import org.junit.Assert;
import org.junit.Test;

public class TopicQueueMappingUtilsTest {


    private Set<String> buildTargetBrokers(int num) {
        return buildTargetBrokers(num, "");
    }

    private Set<String> buildTargetBrokers(int num, String suffix) {
        Set<String> brokers = new HashSet<>();
        for (int i = 0; i < num; i++) {
            brokers.add("broker" + suffix + i);
        }
        return brokers;
    }

    private Map<String, Integer> buildBrokerNumMap(int num) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < num; i++) {
            map.put("broker" + i, 0);
        }
        return map;
    }

    private Map<String, Integer> buildBrokerNumMap(int num, int queues) {
        Map<String, Integer> map = new HashMap<>();
        int random = new Random().nextInt(num);
        for (int i = 0; i < num; i++) {
            map.put("broker" + i, queues);
            if (i == random) {
                map.put("broker" + i, queues + 1);
            }
        }
        return map;
    }

    private void testIdToBroker(Map<Integer, String> idToBroker, Map<String, Integer> brokerNumMap) {
        Map<String, Integer> brokerNumOther = new HashMap<>();
        for (int i = 0; i < idToBroker.size(); i++) {
            Assert.assertTrue(idToBroker.containsKey(i));
            String broker = idToBroker.get(i);
            if (brokerNumOther.containsKey(broker)) {
                brokerNumOther.put(broker, brokerNumOther.get(broker) + 1);
            } else {
                brokerNumOther.put(broker, 1);
            }
        }
        Assert.assertEquals(brokerNumMap.size(), brokerNumOther.size());
        for (Map.Entry<String, Integer> entry: brokerNumOther.entrySet()) {
            Assert.assertEquals(entry.getValue(), brokerNumMap.get(entry.getKey()));
        }
    }

    @Test
    public void testAllocator() {
        //stability
        for (int i = 0; i < 10; i++) {
            int num = 3;
            Map<String, Integer> brokerNumMap = buildBrokerNumMap(num);
            TopicQueueMappingUtils.MappingAllocator  allocator = TopicQueueMappingUtils.buildMappingAllocator(new HashMap<>(), brokerNumMap,  null);
            allocator.upToNum(num * 2);
            for (Map.Entry<String, Integer> entry: allocator.getBrokerNumMap().entrySet()) {
                Assert.assertEquals(2L, entry.getValue().longValue());
            }
            Assert.assertEquals(num * 2, allocator.getIdToBroker().size());
            testIdToBroker(allocator.idToBroker, allocator.getBrokerNumMap());

            allocator.upToNum(num * 3 - 1);

            for (Map.Entry<String, Integer> entry: allocator.getBrokerNumMap().entrySet()) {
                Assert.assertTrue(entry.getValue() >= 2);
                Assert.assertTrue(entry.getValue() <= 3);
            }
            Assert.assertEquals(num * 3 - 1, allocator.getIdToBroker().size());
            testIdToBroker(allocator.idToBroker, allocator.getBrokerNumMap());
        }
    }

    @Test
    public void testRemappingAllocator() {
        for (int i = 0; i < 10; i++) {
            int num = (i + 2) * 2;
            Map<String, Integer> brokerNumMap = buildBrokerNumMap(num);
            Map<String, Integer> brokerNumMapBeforeRemapping = buildBrokerNumMap(num, num);
            TopicQueueMappingUtils.MappingAllocator  allocator = TopicQueueMappingUtils.buildMappingAllocator(new HashMap<>(), brokerNumMap, brokerNumMapBeforeRemapping);
            allocator.upToNum(num * num + 1);
            Assert.assertEquals(brokerNumMapBeforeRemapping, allocator.getBrokerNumMap());
        }
    }


    @Test(expected = RuntimeException.class)
    public void testTargetBrokersComplete() {
        String topic = "static";
        String broker1 = "broker1";
        String broker2 = "broker2";
        Set<String> targetBrokers = new HashSet<>();
        targetBrokers.add(broker1);
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(topic, 0, broker2, 0);
        mappingDetail.getHostedQueues().put(1, new ArrayList<>());
        brokerConfigMap.put(broker2, new TopicConfigAndQueueMapping(new TopicConfig(topic, 0, 0), mappingDetail));
        TopicQueueMappingUtils.checkTargetBrokersComplete(targetBrokers, brokerConfigMap);
    }



    @Test
    public void testCreateStaticTopic() {
        String topic = "static";
        int queueNum;
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        for (int i = 1; i < 10; i++) {
            Set<String> targetBrokers = buildTargetBrokers(2 * i);
            Set<String> nonTargetBrokers = buildTargetBrokers(2 * i, "test");
            queueNum = 10 * i;
            TopicRemappingDetailWrapper wrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);
            Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
            Assert.assertEquals(2 * i, brokerConfigMap.size());

            //do the check manually
            Map.Entry<Long, Integer> maxEpochAndNum = TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            Assert.assertEquals(queueNum, maxEpochAndNum.getValue().longValue());
            Map<Integer, TopicQueueMappingOne> globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(globalIdMap.values());
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);

            for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
                TopicConfigAndQueueMapping configMapping = entry.getValue();
                if (nonTargetBrokers.contains(configMapping.getMappingDetail().bname)) {
                    Assert.assertEquals(0, configMapping.getReadQueueNums());
                    Assert.assertEquals(0, configMapping.getWriteQueueNums());
                    Assert.assertEquals(0, configMapping.getMappingDetail().getHostedQueues().size());
                } else {
                    Assert.assertEquals(5, configMapping.getReadQueueNums());
                    Assert.assertEquals(5, configMapping.getWriteQueueNums());
                    Assert.assertTrue(configMapping.getMappingDetail().epoch > System.currentTimeMillis());
                    for (List<LogicQueueMappingItem> items: configMapping.getMappingDetail().getHostedQueues().values()) {
                        for (LogicQueueMappingItem item: items) {
                            Assert.assertEquals(0, item.getStartOffset());
                            Assert.assertEquals(0, item.getLogicOffset());
                            TopicConfig topicConfig = brokerConfigMap.get(item.getBname());
                            Assert.assertTrue(item.getQueueId() < topicConfig.getWriteQueueNums());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testRemappingStaticTopic() {
        String topic = "static";
        int queueNum = 7;
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        Set<String>  originalBrokers = buildTargetBrokers(2);
        TopicRemappingDetailWrapper wrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, originalBrokers, brokerConfigMap);
        Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
        Assert.assertEquals(2, brokerConfigMap.size());

        {
            //do the check manually
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
            Map<Integer, TopicQueueMappingOne> globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(globalIdMap.values());
        }

        for (int i = 0; i < 10; i++) {
            Set<String> targetBrokers = buildTargetBrokers(2, "test" + i);
            TopicQueueMappingUtils.remappingStaticTopic(topic, brokerConfigMap, targetBrokers);
            //do the check manually
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
            Map<Integer, TopicQueueMappingOne> globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(globalIdMap.values());
            TopicQueueMappingUtils.checkLeaderInTargetBrokers(globalIdMap.values(), targetBrokers);

            Assert.assertEquals((i + 2) * 2, brokerConfigMap.size());

            //check and complete the logicOffset
            for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
                TopicConfigAndQueueMapping configMapping = entry.getValue();
                if (!targetBrokers.contains(configMapping.getMappingDetail().bname)) {
                    continue;
                }
                for (List<LogicQueueMappingItem> items: configMapping.getMappingDetail().getHostedQueues().values()) {
                    Assert.assertEquals(i + 2, items.size());
                    items.get(items.size() - 1).setLogicOffset(i + 1);
                }
            }
        }
    }

    @Test
    public void testRemappingStaticTopicStability() {
        String topic = "static";
        int queueNum = 7;
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        Set<String>  originalBrokers = buildTargetBrokers(2);
        {
            TopicRemappingDetailWrapper wrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, originalBrokers,  brokerConfigMap);
            Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
            Assert.assertEquals(2, brokerConfigMap.size());
        }
        for (int i = 0; i < 10; i++) {
            TopicRemappingDetailWrapper wrapper = TopicQueueMappingUtils.remappingStaticTopic(topic, brokerConfigMap, originalBrokers);
            Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
            Assert.assertEquals(2, brokerConfigMap.size());
            Assert.assertTrue(wrapper.getBrokerToMapIn().isEmpty());
            Assert.assertTrue(wrapper.getBrokerToMapOut().isEmpty());
        }
    }



    @Test
    public void testUtilsCheck() {
        String topic = "static";
        int queueNum = 10;
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        Set<String> targetBrokers = buildTargetBrokers(2);
        TopicRemappingDetailWrapper wrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);
        Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
        Assert.assertEquals(2, brokerConfigMap.size());
        TopicConfigAndQueueMapping configMapping = brokerConfigMap.values().iterator().next();
        List<LogicQueueMappingItem> items = configMapping.getMappingDetail().getHostedQueues().values().iterator().next();
        Map.Entry<Long, Integer> maxEpochNum = TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        int exceptionNum = 0;
        try {
            configMapping.getMappingDetail().setTopic("xxxx");
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().setTopic(topic);
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        }

        try {
            configMapping.getMappingDetail().setTotalQueues(1);
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().setTotalQueues(10);
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        }

        try {
            configMapping.getMappingDetail().setEpoch(0);
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().setEpoch(maxEpochNum.getKey());
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
        }


        try {
            configMapping.getMappingDetail().getHostedQueues().put(10000, new ArrayList<>(Collections.singletonList(new LogicQueueMappingItem(1, 1, targetBrokers.iterator().next(), 0, 0, -1, -1, -1))));
            TopicQueueMappingUtils.checkAndBuildMappingItems(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().getHostedQueues().remove(10000);
            TopicQueueMappingUtils.checkAndBuildMappingItems(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
        }

        try {
            configMapping.setWriteQueueNums(1);
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.setWriteQueueNums(5);
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);
        }

        try {
            items.add(new LogicQueueMappingItem(1, 1, targetBrokers.iterator().next(), 0, 0, -1, -1, -1));
            Map<Integer, TopicQueueMappingOne> map = TopicQueueMappingUtils.checkAndBuildMappingItems(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(map.values());
        } catch (RuntimeException ignore) {
            exceptionNum++;
            items.remove(items.size() - 1);
            Map<Integer, TopicQueueMappingOne> map = TopicQueueMappingUtils.checkAndBuildMappingItems(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values()), false, true);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(map.values());
        }
        Assert.assertEquals(6, exceptionNum);

    }
}
