package org.apache.rocketmq.common.statictopic;

import org.apache.rocketmq.common.TopicConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopicMappingUtilsTest {



    private Set<String> buildTargetBrokers(int num) {
        Set<String> brokers = new HashSet<String>();
        for (int i = 0; i < num; i++) {
            brokers.add("broker" + i);
        }
        return brokers;
    }

    private Map<String, Integer> buildBrokerNumMap(int num) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < num; i++) {
            map.put("broker" + i, 0);
        }
        return map;
    }

    private void testIdToBroker(Map<Integer, String> idToBroker, Map<String, Integer> brokerNumMap) {
        Map<String, Integer> brokerNumOther = new HashMap<String, Integer>();
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
            TopicQueueMappingUtils.MappingAllocator  allocator = TopicQueueMappingUtils.buildMappingAllocator(new HashMap<Integer, String>(), brokerNumMap);
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


    @Test(expected = RuntimeException.class)
    public void testTargetBrokersComplete() {
        String topic = "static";
        String broker1 = "broker1";
        String broker2 = "broker2";
        Set<String> targetBrokers = new HashSet<String>();
        targetBrokers.add(broker1);
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<String, TopicConfigAndQueueMapping>();
        brokerConfigMap.put(broker2, new TopicConfigAndQueueMapping(new TopicConfig(topic, 0, 0), new TopicQueueMappingDetail(topic, 0, broker2, 0)));
        TopicQueueMappingUtils.checkIfTargetBrokersComplete(targetBrokers, brokerConfigMap);
    }



    @Test
    public void testCreateStaticTopic() {
        String topic = "static";
        int queueNum;
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<String, TopicConfigAndQueueMapping>();
        for (int i = 1; i < 10; i++) {
            Set<String> targetBrokers = buildTargetBrokers(2 * i);
            queueNum = 10 * i;
            TopicRemappingDetailWrapper wrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);
            Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
            Assert.assertEquals(2 * i, brokerConfigMap.size());

            //do the check manually
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
            Map<Integer, TopicQueueMappingOne> globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<TopicQueueMappingDetail>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
            TopicQueueMappingUtils.checkIfReusePhysicalQueue(globalIdMap.values());
            TopicQueueMappingUtils.checkPhysicalQueueConsistence(brokerConfigMap);

            for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
                TopicConfigAndQueueMapping configMapping = entry.getValue();
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

    @Test
    public void testCreateStaticTopic_Error() {
        String topic = "static";
        int queueNum = 10;
        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<String, TopicConfigAndQueueMapping>();
        Set<String> targetBrokers = buildTargetBrokers(2);
        TopicRemappingDetailWrapper wrapper  = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);
        Assert.assertEquals(wrapper.getBrokerConfigMap(), brokerConfigMap);
        Assert.assertEquals(2, brokerConfigMap.size());
        TopicConfigAndQueueMapping configMapping = brokerConfigMap.values().iterator().next();
        List<LogicQueueMappingItem> items = configMapping.getMappingDetail().getHostedQueues().values().iterator().next();
        Map.Entry<Long, Integer> maxEpochNum = TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        int exceptionNum = 0;
        try {
            configMapping.getMappingDetail().setTopic("xxxx");
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().setTopic(topic);
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        }

        try {
            configMapping.getMappingDetail().setTotalQueues(1);
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().setTotalQueues(10);
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        }

        try {
            configMapping.getMappingDetail().setEpoch(0);
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        } catch (RuntimeException ignore) {
            exceptionNum++;
            configMapping.getMappingDetail().setEpoch(maxEpochNum.getKey());
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
        }


        try {
            configMapping.getMappingDetail().getHostedQueues().put(10000, new ArrayList<LogicQueueMappingItem>(Collections.singletonList(new LogicQueueMappingItem(1, 1, targetBrokers.iterator().next(), 0, 0, -1, -1, -1))));
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
