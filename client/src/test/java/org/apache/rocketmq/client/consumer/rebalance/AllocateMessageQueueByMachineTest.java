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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocateMessageQueueByMachineTest {
    private final AllocateMessageQueueStrategy strategy = new AllocateMessageQueueAveragely();
    private final AllocateMessageQueueByMachine allocateByMachine = new AllocateMessageQueueByMachine(strategy);
    private TopicRouteData topicRouteData = null;
    private final List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
    private final List<String> cidAll = new ArrayList<String>();

    /**
     * 4 messageQueue
     * 4 cid
     */
    public void init1() {
        MessageQueue mq0 = new MessageQueue("mockTopic", "broker-a", 0);
        MessageQueue mq1 = new MessageQueue("mockTopic", "broker-a", 1);

        mqAll.add(mq0);
        mqAll.add(mq1);

        MessageQueue mq10 = new MessageQueue("mockTopic", "broker-b", 0);
        MessageQueue mq11 = new MessageQueue("mockTopic", "broker-b", 1);
        mqAll.add(mq10);
        mqAll.add(mq11);

        TopicRouteData routeData1 = new TopicRouteData();

        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-a");
        queueData1.setPerm(6);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);
        queueData1.setTopicSysFlag(0);

        List<QueueData> arrayList = new ArrayList<QueueData>();
        arrayList.add(queueData1);

        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-b");
        queueData2.setPerm(6);
        queueData2.setReadQueueNums(2);
        queueData2.setWriteQueueNums(2);
        queueData2.setTopicSysFlag(0);

        arrayList.add(queueData2);

        routeData1.setQueueDatas(arrayList);

        List<BrokerData> brokerDataList1 = new ArrayList<BrokerData>();
        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-a");
        brokerData1.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "30.250.200.103:10911");
        brokerAddrs1.put(1L, "30.250.200.102:10911");
        brokerData1.setBrokerAddrs(brokerAddrs1);

        brokerDataList1.add(brokerData1);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("broker-b");
        brokerData2.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "30.250.200.109:10911");
        brokerAddrs2.put(1L, "30.250.200.108:10911");
        brokerData2.setBrokerAddrs(brokerAddrs2);

        brokerDataList1.add(brokerData2);


        routeData1.setBrokerDatas(brokerDataList1);

        topicRouteData = routeData1;

        cidAll.add("30.250.200.103@1000");
        cidAll.add("30.250.200.107@1000");
        cidAll.add("30.250.200.104@1000");
        cidAll.add("30.250.200.109@1000");
    }

    @Test
    public void test1(){
        init1();

        List<MessageQueue> allocate1 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.103@1000", mqAll, cidAll);

        System.out.println("=======30.250.200.103@1000======");
        for (MessageQueue messageQueue : allocate1) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate1.size());
        Assert.assertEquals("broker-a",allocate1.get(0).getBrokerName());

        System.out.println("======30.250.200.107@1000=======");
        List<MessageQueue> allocate2 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.107@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate2) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate2.size());


        System.out.println("=======30.250.200.104@1000======");
        List<MessageQueue> allocate3 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.104@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate3) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate3.size());

        System.out.println("======30.250.200.109@1000=======");
        List<MessageQueue> allocate4 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.109@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate4) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate4.size());
        Assert.assertEquals("broker-b",allocate4.get(0).getBrokerName());
    }


    /**
     * 9 messageQueue
     * 4 cid
     */
    public void init2() {
        MessageQueue mq0 = new MessageQueue("mockTopic", "broker-a", 0);
        MessageQueue mq1 = new MessageQueue("mockTopic", "broker-a", 1);
        MessageQueue mq2 = new MessageQueue("mockTopic", "broker-a", 2);
        MessageQueue mq3 = new MessageQueue("mockTopic", "broker-a", 3);
        MessageQueue mq4 = new MessageQueue("mockTopic", "broker-a", 4);
        MessageQueue mq5 = new MessageQueue("mockTopic", "broker-a", 5);

        mqAll.add(mq0);
        mqAll.add(mq1);
        mqAll.add(mq2);
        mqAll.add(mq3);
        mqAll.add(mq4);
        mqAll.add(mq5);

        MessageQueue mq10 = new MessageQueue("mockTopic", "broker-b", 0);
        MessageQueue mq11 = new MessageQueue("mockTopic", "broker-b", 1);
        MessageQueue mq12 = new MessageQueue("mockTopic", "broker-b", 2);
        mqAll.add(mq10);
        mqAll.add(mq11);
        mqAll.add(mq12);

        TopicRouteData routeData1 = new TopicRouteData();

        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-a");
        queueData1.setPerm(6);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);
        queueData1.setTopicSysFlag(0);

        List<QueueData> arrayList = new ArrayList<QueueData>();
        arrayList.add(queueData1);

        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-b");
        queueData2.setPerm(6);
        queueData2.setReadQueueNums(2);
        queueData2.setWriteQueueNums(2);
        queueData2.setTopicSysFlag(0);

        arrayList.add(queueData2);

        routeData1.setQueueDatas(arrayList);

        List<BrokerData> brokerDataList1 = new ArrayList<BrokerData>();
        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-a");
        brokerData1.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "30.250.200.103:10911");
        brokerAddrs1.put(1L, "30.250.200.102:10911");
        brokerData1.setBrokerAddrs(brokerAddrs1);

        brokerDataList1.add(brokerData1);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("broker-b");
        brokerData2.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "30.250.200.109:10911");
        brokerAddrs2.put(1L, "30.250.200.108:10911");
        brokerData2.setBrokerAddrs(brokerAddrs2);

        brokerDataList1.add(brokerData2);


        routeData1.setBrokerDatas(brokerDataList1);

        topicRouteData = routeData1;


        cidAll.add("30.250.200.103@1000");
        cidAll.add("30.250.200.107@1000");
        cidAll.add("30.250.200.104@1000");
        cidAll.add("30.250.200.109@1000");
    }

    @Test
    public void test2() {
        init2();

        List<MessageQueue> allocate1 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.103@1000", mqAll, cidAll);

        System.out.println("=======30.250.200.103@1000======");
        for (MessageQueue messageQueue : allocate1) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(2, allocate1.size());
        Assert.assertEquals("broker-a",allocate1.get(0).getBrokerName());
        Assert.assertEquals("broker-a",allocate1.get(1).getBrokerName());

        System.out.println("======30.250.200.107@1000=======");
        List<MessageQueue> allocate2 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.107@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate2) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(2, allocate2.size());


        System.out.println("=======30.250.200.104@1000======");
        List<MessageQueue> allocate3 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.104@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate3) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(2, allocate3.size());

        System.out.println("======30.250.200.109@1000=======");
        List<MessageQueue> allocate4 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.109@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate4) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(3, allocate4.size());
        Assert.assertEquals("broker-b",allocate4.get(0).getBrokerName());
        Assert.assertEquals("broker-b",allocate4.get(1).getBrokerName());
        Assert.assertEquals("broker-b",allocate4.get(2).getBrokerName());
    }

    /**
     * 4 messageQueue
     * 3 cid
     */
    public void init3() {
        MessageQueue mq0 = new MessageQueue("mockTopic", "broker-a", 0);
        MessageQueue mq1 = new MessageQueue("mockTopic", "broker-a", 1);

        mqAll.add(mq0);
        mqAll.add(mq1);

        MessageQueue mq10 = new MessageQueue("mockTopic", "broker-b", 0);
        MessageQueue mq11 = new MessageQueue("mockTopic", "broker-b", 1);
        mqAll.add(mq10);
        mqAll.add(mq11);

        TopicRouteData routeData1 = new TopicRouteData();

        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-a");
        queueData1.setPerm(6);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);
        queueData1.setTopicSysFlag(0);

        List<QueueData> arrayList = new ArrayList<QueueData>();
        arrayList.add(queueData1);

        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-b");
        queueData2.setPerm(6);
        queueData2.setReadQueueNums(2);
        queueData2.setWriteQueueNums(2);
        queueData2.setTopicSysFlag(0);

        arrayList.add(queueData2);

        routeData1.setQueueDatas(arrayList);

        List<BrokerData> brokerDataList1 = new ArrayList<BrokerData>();
        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-a");
        brokerData1.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "30.250.200.103:10911");
        brokerAddrs1.put(1L, "30.250.200.102:10911");
        brokerData1.setBrokerAddrs(brokerAddrs1);

        brokerDataList1.add(brokerData1);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("broker-b");
        brokerData2.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "30.250.200.109:10911");
        brokerAddrs2.put(1L, "30.250.200.108:10911");
        brokerData2.setBrokerAddrs(brokerAddrs2);

        brokerDataList1.add(brokerData2);


        routeData1.setBrokerDatas(brokerDataList1);

        topicRouteData = routeData1;

        cidAll.add("30.250.200.103@1000");
        cidAll.add("30.250.200.107@1000");
        cidAll.add("30.250.200.104@1000");
    }


    @Test
    public void test3(){
        init3();

        List<MessageQueue> allocate1 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.103@1000", mqAll, cidAll);

        System.out.println("=======30.250.200.103@1000======");
        for (MessageQueue messageQueue : allocate1) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(2, allocate1.size());
        Assert.assertEquals("broker-a",allocate1.get(0).getBrokerName());
        Assert.assertEquals("broker-a",allocate1.get(1).getBrokerName());

        System.out.println("======30.250.200.107@1000=======");
        List<MessageQueue> allocate2 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.107@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate2) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate2.size());


        System.out.println("=======30.250.200.104@1000======");
        List<MessageQueue> allocate3 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.104@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate3) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate3.size());

    }

    /**
     * 4 messageQueue
     * 5 cid
     */
    public void init4() {
        MessageQueue mq0 = new MessageQueue("mockTopic", "broker-a", 0);
        MessageQueue mq1 = new MessageQueue("mockTopic", "broker-a", 1);

        mqAll.add(mq0);
        mqAll.add(mq1);

        MessageQueue mq10 = new MessageQueue("mockTopic", "broker-b", 0);
        MessageQueue mq11 = new MessageQueue("mockTopic", "broker-b", 1);
        mqAll.add(mq10);
        mqAll.add(mq11);

        TopicRouteData routeData1 = new TopicRouteData();

        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-a");
        queueData1.setPerm(6);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);
        queueData1.setTopicSysFlag(0);

        List<QueueData> arrayList = new ArrayList<QueueData>();
        arrayList.add(queueData1);

        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-b");
        queueData2.setPerm(6);
        queueData2.setReadQueueNums(2);
        queueData2.setWriteQueueNums(2);
        queueData2.setTopicSysFlag(0);

        arrayList.add(queueData2);

        routeData1.setQueueDatas(arrayList);

        List<BrokerData> brokerDataList1 = new ArrayList<BrokerData>();
        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-a");
        brokerData1.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "30.250.200.103:10911");
        brokerAddrs1.put(1L, "30.250.200.102:10911");
        brokerData1.setBrokerAddrs(brokerAddrs1);

        brokerDataList1.add(brokerData1);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("broker-b");
        brokerData2.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "30.250.200.109:10911");
        brokerAddrs2.put(1L, "30.250.200.108:10911");
        brokerData2.setBrokerAddrs(brokerAddrs2);

        brokerDataList1.add(brokerData2);


        routeData1.setBrokerDatas(brokerDataList1);

        topicRouteData = routeData1;


        cidAll.add("30.250.200.103@1000");
        cidAll.add("30.250.200.107@1000");
        cidAll.add("30.250.200.104@1000");
        cidAll.add("30.250.200.109@1000");
        cidAll.add("30.250.200.110@1000");
    }

    @Test
    public void test4() {
        init4();

        List<MessageQueue> allocate1 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.103@1000", mqAll, cidAll);

        System.out.println("=======30.250.200.103@1000======");
        for (MessageQueue messageQueue : allocate1) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate1.size());
        Assert.assertEquals("broker-a",allocate1.get(0).getBrokerName());

        System.out.println("======30.250.200.107@1000=======");
        List<MessageQueue> allocate2 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.107@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate2) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate2.size());


        System.out.println("=======30.250.200.104@1000======");
        List<MessageQueue> allocate3 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.104@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate3) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate3.size());

        System.out.println("======30.250.200.109@1000=======");
        List<MessageQueue> allocate4 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.109@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate4) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate4.size());
        Assert.assertEquals("broker-b",allocate4.get(0).getBrokerName());

        System.out.println("======30.250.200.110@1000=======");
        List<MessageQueue> allocate5 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.110@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate5) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(0, allocate5.size());
    }

    /**
     * 6 messageQueue
     * 6 cid
     */
    public void init5() {
        MessageQueue mq0 = new MessageQueue("mockTopic", "broker-a", 0);
        MessageQueue mq1 = new MessageQueue("mockTopic", "broker-a", 1);

        mqAll.add(mq0);
        mqAll.add(mq1);

        MessageQueue mq10 = new MessageQueue("mockTopic", "broker-b", 0);
        MessageQueue mq11 = new MessageQueue("mockTopic", "broker-b", 1);
        mqAll.add(mq10);
        mqAll.add(mq11);

        MessageQueue mq20 = new MessageQueue("mockTopic", "broker-c", 0);
        MessageQueue mq21 = new MessageQueue("mockTopic", "broker-c", 1);
        mqAll.add(mq20);
        mqAll.add(mq21);

        TopicRouteData routeData = new TopicRouteData();
        List<QueueData> arrayList = new ArrayList<QueueData>();
        routeData.setQueueDatas(arrayList);

        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        routeData.setBrokerDatas(brokerDataList);


        //1
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-a");
        queueData1.setPerm(6);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);
        queueData1.setTopicSysFlag(0);
        arrayList.add(queueData1);

        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-a");
        brokerData1.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "30.250.200.103:10911");
        brokerAddrs1.put(1L, "30.250.200.102:10911");
        brokerData1.setBrokerAddrs(brokerAddrs1);
        brokerDataList.add(brokerData1);

        //2
        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-b");
        queueData2.setPerm(6);
        queueData2.setReadQueueNums(2);
        queueData2.setWriteQueueNums(2);
        queueData2.setTopicSysFlag(0);
        arrayList.add(queueData2);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("broker-b");
        brokerData2.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "30.250.200.109:10911");
        brokerAddrs2.put(1L, "30.250.200.108:10911");
        brokerData2.setBrokerAddrs(brokerAddrs2);
        brokerDataList.add(brokerData2);

        //3
        QueueData queueData3 = new QueueData();
        queueData3.setBrokerName("broker-c");
        queueData3.setPerm(6);
        queueData3.setReadQueueNums(2);
        queueData3.setWriteQueueNums(2);
        queueData3.setTopicSysFlag(0);
        arrayList.add(queueData3);

        BrokerData brokerData3 = new BrokerData();
        brokerData3.setBrokerName("broker-c");
        brokerData3.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs3 = new HashMap<Long, String>();
        brokerAddrs3.put(0L, "30.250.200.120:10911");
        brokerAddrs3.put(1L, "30.250.200.121:10911");
        brokerData3.setBrokerAddrs(brokerAddrs3);
        brokerDataList.add(brokerData3);


        topicRouteData = routeData;

        cidAll.add("30.250.200.103@1000");
        cidAll.add("30.250.200.107@1000");
        cidAll.add("30.250.200.104@1000");
        cidAll.add("30.250.200.109@1000");
        cidAll.add("30.250.200.105@1000");
        cidAll.add("30.250.200.106@1000");

    }

    @Test
    public void test5(){
        init5();

        List<MessageQueue> allocate1 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.103@1000", mqAll, cidAll);

        System.out.println("=======30.250.200.103@1000======");
        for (MessageQueue messageQueue : allocate1) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate1.size());
        Assert.assertEquals("broker-a",allocate1.get(0).getBrokerName());

        System.out.println("======30.250.200.107@1000=======");
        List<MessageQueue> allocate2 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.107@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate2) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate2.size());


        System.out.println("=======30.250.200.104@1000======");
        List<MessageQueue> allocate3 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.104@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate3) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate3.size());

        System.out.println("======30.250.200.109@1000=======");
        List<MessageQueue> allocate4 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.109@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate4) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate4.size());
        Assert.assertEquals("broker-b",allocate4.get(0).getBrokerName());

        System.out.println("======30.250.200.105@1000=======");
        List<MessageQueue> allocate5 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.105@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate5) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate4.size());

        System.out.println("======30.250.200.106@1000=======");
        List<MessageQueue> allocate6 = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.106@1000", mqAll, cidAll);
        for (MessageQueue messageQueue : allocate6) {
            System.out.println(messageQueue);
        }
        Assert.assertEquals(1, allocate4.size());
    }
}
