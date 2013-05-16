package com.alibaba.rocketmq.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;


public class TopicRuntimeDataTest {

    @Test
    public void testEncode() throws Exception {
        TopicRuntimeData topicRuntimeData = create();
        byte[] data = topicRuntimeData.encode();
        TopicRuntimeData serial = TopicRuntimeData.decode(data);
        
        Assert.assertTrue(topicRuntimeData.equals(serial));
    }
    
    @Test
    public void testEncodeSpecific() throws Exception {
        TopicRuntimeData topicRuntimeData = createSpecific();
        byte[] data = topicRuntimeData.encodeSpecific();
        TopicRuntimeData serial = TopicRuntimeData.decode(data);
        
        Assert.assertTrue(topicRuntimeData.equals(serial));
    }

    
    private TopicRuntimeData createSpecific() {
        TopicRuntimeData data = new TopicRuntimeData();

        data.getTopicOrderConfs().put("topic-1",
            "topic.num.DAILY-UIC-USERS-EXTRA=105:4;106:4;topic.num.andor-applye-test=100:2;101:2");

        data.getBrokerList().add("meta://10.23.12.12:8123");
        data.getBrokerList().add("meta://10.23.12.13:8123");
        data.getBrokerList().add("meta://10.23.12.14:8123");
        data.getBrokerList().add("meta://10.23.12.15:8123");

        return data;
    }
    

    private TopicRuntimeData create() {
        TopicRuntimeData data = new TopicRuntimeData();
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-1");
        queueData1.setPerm(4);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);

        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-2");
        queueData2.setPerm(4);
        queueData2.setReadQueueNums(4);
        queueData2.setWriteQueueNums(4);

        QueueData queueData3 = new QueueData();
        queueData3.setBrokerName("broker-3");
        queueData3.setPerm(4);
        queueData3.setReadQueueNums(4);
        queueData3.setWriteQueueNums(4);

        QueueData queueData4 = new QueueData();
        queueData4.setBrokerName("broker-4");
        queueData4.setPerm(4);
        queueData4.setReadQueueNums(4);
        queueData4.setWriteQueueNums(4);

        List<QueueData> queueDatas1 = new ArrayList<QueueData>();
        queueDatas1.add(queueData1);
        queueDatas1.add(queueData2);

        List<QueueData> queueDatas2 = new ArrayList<QueueData>();
        queueDatas2.add(queueData3);
        queueDatas2.add(queueData4);

        data.getTopicBrokers().put("topic-1", queueDatas1);
        data.getTopicBrokers().put("topic-2", queueDatas2);

        data.getTopicOrderConfs().put("topic-1",
            "topic.num.DAILY-UIC-USERS-EXTRA=105:4;106:4;topic.num.andor-applye-test=100:2;101:2");

        BrokerData broker1 = new BrokerData();
        broker1.setBrokerName("broker-1");
        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "meta://10.23.12.12:8123");
        broker1.setBrokerAddrs(brokerAddrs1);

        BrokerData broker2 = new BrokerData();
        broker2.setBrokerName("broker-2");
        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "meta://10.23.12.13:8123");
        broker2.setBrokerAddrs(brokerAddrs2);

        BrokerData broker3 = new BrokerData();
        broker3.setBrokerName("broker-3");
        HashMap<Long, String> brokerAddrs3 = new HashMap<Long, String>();
        brokerAddrs3.put(0L, "meta://10.23.12.14:8123");
        broker3.setBrokerAddrs(brokerAddrs3);

        BrokerData broker4 = new BrokerData();
        broker4.setBrokerName("broker-4");
        HashMap<Long, String> brokerAddrs4 = new HashMap<Long, String>();
        //brokerAddrs4.put(0L, "meta://10.23.12.15:8123");
        broker4.setBrokerAddrs(brokerAddrs4);

        data.getBrokers().put("broker-1", broker1);
        data.getBrokers().put("broker-2", broker2);
        data.getBrokers().put("broker-3", broker3);
        data.getBrokers().put("broker-4", broker4);

        data.getBrokerList().add("meta://10.23.12.12:8123");
        data.getBrokerList().add("meta://10.23.12.13:8123");
        data.getBrokerList().add("meta://10.23.12.14:8123");
        data.getBrokerList().add("meta://10.23.12.15:8123");

        return data;
    }
}
