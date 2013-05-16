package com.alibaba.rocketmq.namesrv;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import com.alibaba.rocketmq.common.namesrv.TopicRuntimeData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;


/**
 * @auther lansheng.zj@taobao.com
 */
public class DataUtils {

    public static TopicRuntimeData createSpecific1() {
        TopicRuntimeData data = new TopicRuntimeData();

        data.getTopicOrderConfs().put("topic.num.topic-1", "105:4;106:4");
        data.getTopicOrderConfs().put("topic.num.andor-applye-test", "105:4;106:4");

        data.getBrokerList().add("meta://10.23.12.12:8123");
        data.getBrokerList().add("meta://10.23.12.13:8123");
        data.getBrokerList().add("meta://10.23.12.14:8123");
        data.getBrokerList().add("meta://10.23.12.15:8123");

        return data;
    }


    public static TopicRuntimeData createSpecific2() {
        TopicRuntimeData data = new TopicRuntimeData();

        data.getTopicOrderConfs().put("topic.num.topic-1", "105:4;106:4");
        data.getTopicOrderConfs().put("topic.num.andor-applye-test", "105:4;106:4");

        data.getBrokerList().add("meta://10.23.12.12:8123");
        data.getBrokerList().add("meta://10.23.12.13:8123");
        data.getBrokerList().add("meta://10.23.12.100:8123");
        data.getBrokerList().add("meta://10.23.12.101:8123");

        return data;
    }


    public static TopicRuntimeData createExpect() {
        TopicRuntimeData data = new TopicRuntimeData();

        data.getTopicOrderConfs().put("topic.num.topic-1", "105:4;106:4");
        data.getTopicOrderConfs().put("topic.num.andor-applye-test", "105:4;106:4");

        data.getBrokerList().add("meta://10.23.12.12:8123");
        data.getBrokerList().add("meta://10.23.12.13:8123");
        data.getBrokerList().add("meta://10.23.12.14:8123");
        data.getBrokerList().add("meta://10.23.12.15:8123");
        data.getBrokerList().add("meta://10.23.12.100:8123");
        data.getBrokerList().add("meta://10.23.12.101:8123");

        return data;
    }
    
    
    public static TopicRuntimeData createOne() {
        TopicRuntimeData data = new TopicRuntimeData();
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-1");
        queueData1.setPerm(4);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);

        List<QueueData> queueDatas1 = new ArrayList<QueueData>();
        queueDatas1.add(queueData1);

        List<QueueData> queueDatas2 = new ArrayList<QueueData>();
        queueDatas2.add(queueData1);

        data.getTopicBrokers().put("topic-1", queueDatas1);
        data.getTopicBrokers().put("topic-2", queueDatas2);

        data.getTopicOrderConfs().put("topic.num.topic-1", "105:4;106:4");
        data.getTopicOrderConfs().put("topic.num.andor-applye-test", "105:4;106:4");

        BrokerData broker1 = new BrokerData();
        broker1.setBrokerName("broker-1");
        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "meta://10.23.12.12:8123");
        broker1.setBrokerAddrs(brokerAddrs1);

        data.getBrokers().put("broker-1", broker1);

        data.getBrokerList().add("meta://10.23.12.12:8123");
        data.getBrokerList().add("meta://10.23.12.13:8123");
        data.getBrokerList().add("meta://10.23.12.14:8123");
        data.getBrokerList().add("meta://10.23.12.15:8123");

        return data;
    }


    public static TopicRuntimeData create() {
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
        queueDatas2.add(queueData1);
        queueDatas2.add(queueData3);
        queueDatas2.add(queueData4);

        data.getTopicBrokers().put("topic-1", queueDatas1);
        data.getTopicBrokers().put("topic-2", queueDatas2);

        data.getTopicOrderConfs().put("topic.num.topic-1", "105:4;106:4");
        data.getTopicOrderConfs().put("topic.num.andor-applye-test", "105:4;106:4");

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
        // brokerAddrs4.put(0L, "meta://10.23.12.15:8123");
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


    public static QueueData createQueueData() {
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName(createBrokerName());
        queueData1.setPerm(4);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);

        return queueData1;
    }


    public static String createBrokerName() {
        return "broker-" + randomInt(1000);
    }


    public static String createAddr() {
        return "meta://" + randomInt(256) + "." + randomInt(256) + "." + randomInt(256) + "." + randomInt(256)
                + ":8123";
    }


    public static String createTopic() {
        return "topic-" + randomInt(1000);
    }


    public static int randomInt(int ratio) {
        Random random = new Random(System.currentTimeMillis());
        return Math.abs(random.nextInt()) % ratio;
    }


    public static <T, V> void hackField(T target, V value, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }


    public static <T, F> F getField(T target, Class<F> fieldClazz, String fieldName)
            throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        F fieldObj = (F)field.get(target);
        return fieldObj;
    }
}
