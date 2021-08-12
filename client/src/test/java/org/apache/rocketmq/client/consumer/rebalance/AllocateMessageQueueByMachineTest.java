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
    private final AllocateMessageQueueByMachine allocateByMachine = new AllocateMessageQueueByMachine(new AllocateMessageQueueAveragely());
    private final Map<String, TopicRouteData> topicRouteData = new HashMap<String, TopicRouteData>();
    private final List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
    private final List<String> cidAll = new ArrayList<String>();

    @Before
    public void init() {
        MessageQueue mq0 = new MessageQueue("mockTopic", "broker-a", 0);
        MessageQueue mq1 = new MessageQueue("mockTopic", "broker-a", 1);
        MessageQueue mq2 = new MessageQueue("mockTopic", "broker-a", 2);
        MessageQueue mq3 = new MessageQueue("mockTopic", "broker-a", 3);

        mqAll.add(mq0);
        mqAll.add(mq1);
        mqAll.add(mq2);
        mqAll.add(mq3);

        TopicRouteData routeData1 = new TopicRouteData();
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName("broker-a");
        queueData1.setPerm(6);
        queueData1.setReadQueueNums(4);
        queueData1.setWriteQueueNums(4);
        queueData1.setTopicSysFlag(0);
        List<QueueData> arrayList1 = new ArrayList<QueueData>();
        arrayList1.add(queueData1);

        routeData1.setQueueDatas(arrayList1);

        List<BrokerData> brokerDataList1 = new ArrayList<BrokerData>();
        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-a");
        brokerData1.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "30.250.200.103:10911");
        brokerAddrs1.put(1L, "30.250.200.102:10911");

        brokerData1.setBrokerAddrs(brokerAddrs1);

        brokerDataList1.add(brokerData1);

        routeData1.setBrokerDatas(brokerDataList1);

        topicRouteData.put("mockTopic", routeData1);

        //=======================================

        TopicRouteData routeData2 = new TopicRouteData();
        QueueData queueData2 = new QueueData();
        queueData2.setBrokerName("broker-a");
        queueData2.setPerm(6);
        queueData2.setReadQueueNums(4);
        queueData2.setWriteQueueNums(4);
        queueData2.setTopicSysFlag(0);
        List<QueueData> arrayList2 = new ArrayList<QueueData>();
        arrayList2.add(queueData2);

        routeData2.setQueueDatas(arrayList2);

        List<BrokerData> brokerDataList2 = new ArrayList<BrokerData>();
        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("broker-b");
        brokerData2.setCluster("DefaultCluster");

        HashMap<Long, String> brokerAddrs2 = new HashMap<Long, String>();
        brokerAddrs2.put(0L, "30.250.200.104:10911");
        brokerAddrs2.put(1L, "30.250.200.105:10911");

        brokerData2.setBrokerAddrs(brokerAddrs2);

        brokerDataList2.add(brokerData2);
        routeData2.setBrokerDatas(brokerDataList2);
        topicRouteData.put("mockTopic-1", routeData2);

        cidAll.add("30.250.200.103@65862");
        cidAll.add("30.250.200.102@1000");
        cidAll.add("30.250.200.104@1000");
        cidAll.add("30.250.200.105@1000");
    }

    @Test
    public void test1() {
        List<MessageQueue> allocate = allocateByMachine.allocate(topicRouteData, "Test-C-G", "30.250.200.103@65862", mqAll, cidAll);

        Assert.assertEquals(4, allocate.size());
    }

}
