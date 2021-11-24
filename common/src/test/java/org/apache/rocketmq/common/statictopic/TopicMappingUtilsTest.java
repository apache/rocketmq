package org.apache.rocketmq.common.statictopic;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TopicMappingUtilsTest {


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
}
