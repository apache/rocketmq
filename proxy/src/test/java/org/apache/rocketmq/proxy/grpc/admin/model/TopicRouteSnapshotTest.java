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

package org.apache.rocketmq.proxy.grpc.admin.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TopicRouteSnapshotTest {

    @Test
    public void testDefaultConstructor() {
        TopicRouteSnapshot snapshot = new TopicRouteSnapshot();
        assertNull(snapshot.getTopic());
        assertNull(snapshot.getBrokers());
        assertNull(snapshot.getQueues());
    }

    @Test
    public void testSettersAndGetters() {
        TopicRouteSnapshot snapshot = new TopicRouteSnapshot();
        snapshot.setTopic("test-topic");

        List<TopicRouteSnapshot.BrokerInfo> brokers = new ArrayList<>();
        TopicRouteSnapshot.BrokerInfo brokerInfo = new TopicRouteSnapshot.BrokerInfo();
        brokerInfo.setCluster("cluster-a");
        brokerInfo.setBrokerName("broker-0");
        Map<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        brokerInfo.setBrokerAddrs(addrs);
        brokers.add(brokerInfo);
        snapshot.setBrokers(brokers);

        List<TopicRouteSnapshot.QueueInfo> queues = new ArrayList<>();
        TopicRouteSnapshot.QueueInfo queueInfo = new TopicRouteSnapshot.QueueInfo();
        queueInfo.setBrokerName("broker-0");
        queueInfo.setReadQueueNums(8);
        queueInfo.setWriteQueueNums(8);
        queueInfo.setPerm(6);
        queues.add(queueInfo);
        snapshot.setQueues(queues);

        assertEquals("test-topic", snapshot.getTopic());
        assertEquals(1, snapshot.getBrokers().size());
        assertEquals("cluster-a", snapshot.getBrokers().get(0).getCluster());
        assertEquals("broker-0", snapshot.getBrokers().get(0).getBrokerName());
        assertEquals(1, snapshot.getBrokers().get(0).getBrokerAddrs().size());
        assertEquals("127.0.0.1:10911", snapshot.getBrokers().get(0).getBrokerAddrs().get(0L));
        assertEquals(1, snapshot.getQueues().size());
        assertEquals("broker-0", snapshot.getQueues().get(0).getBrokerName());
        assertEquals(8, snapshot.getQueues().get(0).getReadQueueNums());
        assertEquals(8, snapshot.getQueues().get(0).getWriteQueueNums());
        assertEquals(6, snapshot.getQueues().get(0).getPerm());
    }

    @Test
    public void testBrokerInfoDefaultConstructor() {
        TopicRouteSnapshot.BrokerInfo brokerInfo = new TopicRouteSnapshot.BrokerInfo();
        assertNull(brokerInfo.getCluster());
        assertNull(brokerInfo.getBrokerName());
        assertNull(brokerInfo.getBrokerAddrs());
    }

    @Test
    public void testBrokerInfoWithMultipleAddresses() {
        TopicRouteSnapshot.BrokerInfo brokerInfo = new TopicRouteSnapshot.BrokerInfo();
        Map<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        addrs.put(1L, "127.0.0.1:10921");
        brokerInfo.setBrokerAddrs(addrs);

        assertEquals(2, brokerInfo.getBrokerAddrs().size());
        assertEquals("127.0.0.1:10911", brokerInfo.getBrokerAddrs().get(0L));
        assertEquals("127.0.0.1:10921", brokerInfo.getBrokerAddrs().get(1L));
    }

    @Test
    public void testQueueInfoDefaultConstructor() {
        TopicRouteSnapshot.QueueInfo queueInfo = new TopicRouteSnapshot.QueueInfo();
        assertNull(queueInfo.getBrokerName());
        assertEquals(0, queueInfo.getReadQueueNums());
        assertEquals(0, queueInfo.getWriteQueueNums());
        assertEquals(0, queueInfo.getPerm());
    }

    @Test
    public void testQueueInfoSetters() {
        TopicRouteSnapshot.QueueInfo queueInfo = new TopicRouteSnapshot.QueueInfo();
        queueInfo.setBrokerName("broker-x");
        queueInfo.setReadQueueNums(16);
        queueInfo.setWriteQueueNums(16);
        queueInfo.setPerm(4);

        assertEquals("broker-x", queueInfo.getBrokerName());
        assertEquals(16, queueInfo.getReadQueueNums());
        assertEquals(16, queueInfo.getWriteQueueNums());
        assertEquals(4, queueInfo.getPerm());
    }

    @Test
    public void testSnapshotWithMultipleBrokersAndQueues() {
        TopicRouteSnapshot snapshot = new TopicRouteSnapshot();
        snapshot.setTopic("multi-broker-topic");

        List<TopicRouteSnapshot.BrokerInfo> brokers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TopicRouteSnapshot.BrokerInfo brokerInfo = new TopicRouteSnapshot.BrokerInfo();
            brokerInfo.setCluster("cluster-a");
            brokerInfo.setBrokerName("broker-" + i);
            Map<Long, String> addrs = new HashMap<>();
            addrs.put(0L, "127.0.0.1:1091" + i);
            brokerInfo.setBrokerAddrs(addrs);
            brokers.add(brokerInfo);
        }
        snapshot.setBrokers(brokers);

        List<TopicRouteSnapshot.QueueInfo> queues = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TopicRouteSnapshot.QueueInfo queueInfo = new TopicRouteSnapshot.QueueInfo();
            queueInfo.setBrokerName("broker-" + i);
            queueInfo.setReadQueueNums(8);
            queueInfo.setWriteQueueNums(8);
            queueInfo.setPerm(6);
            queues.add(queueInfo);
        }
        snapshot.setQueues(queues);

        assertEquals(3, snapshot.getBrokers().size());
        assertEquals(3, snapshot.getQueues().size());
        for (int i = 0; i < 3; i++) {
            assertEquals("broker-" + i, snapshot.getBrokers().get(i).getBrokerName());
            assertEquals("broker-" + i, snapshot.getQueues().get(i).getBrokerName());
        }
    }
}