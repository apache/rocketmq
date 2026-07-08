/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * The License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEvent;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.proxy.grpc.admin.model.TopicRouteSnapshot;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for RouteChangeEventDetector.
 * Covers: topic create/delete, broker online/offline, queue scaling,
 * address changes, null/empty route handling, and snapshot building.
 */
public class RouteChangeEventDetectorTest {

    private RouteChangeEventDetector detector;

    @Before
    public void setUp() {
        detector = new RouteChangeEventDetector();
    }

    // ==================== Topic Create Tests ====================

    @Test
    public void testDetectChanges_TopicCreate() {
        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", null, newRoute);

        assertEquals(2, events.size());
        assertEquals(RouteChangeEventType.TOPIC_CREATE, events.get(0).getEventType());
        assertEquals("test-topic", events.get(0).getTopic());
        assertEquals(RouteChangeEventType.BROKER_ONLINE, events.get(1).getEventType());
        assertEquals("broker-0", events.get(1).getBrokerName());
        assertEquals(0L, events.get(1).getBrokerId());
        assertEquals("127.0.0.1:10911", events.get(1).getBrokerAddress());
    }

    @Test
    public void testDetectChanges_TopicCreateFromEmptyRoute() {
        TopicRouteData emptyRoute = new TopicRouteData();
        emptyRoute.setBrokerDatas(new ArrayList<>());
        emptyRoute.setQueueDatas(new ArrayList<>());

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", emptyRoute, newRoute);

        assertTrue(events.size() >= 1);
        assertEquals(RouteChangeEventType.TOPIC_CREATE, events.get(0).getEventType());
    }

    // ==================== Topic Delete Tests ====================

    @Test
    public void testDetectChanges_TopicDelete() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, null);

        assertEquals(2, events.size());
        assertEquals(RouteChangeEventType.TOPIC_DELETE, events.get(0).getEventType());
        assertEquals("test-topic", events.get(0).getTopic());
        assertEquals(RouteChangeEventType.BROKER_OFFLINE, events.get(1).getEventType());
        assertEquals("broker-0", events.get(1).getBrokerName());
    }

    @Test
    public void testDetectChanges_TopicDeleteToEmptyRoute() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData emptyRoute = new TopicRouteData();
        emptyRoute.setBrokerDatas(new ArrayList<>());
        emptyRoute.setQueueDatas(new ArrayList<>());

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, emptyRoute);

        assertTrue(events.size() >= 1);
        assertEquals(RouteChangeEventType.TOPIC_DELETE, events.get(0).getEventType());
    }

    // ==================== No Change Tests ====================

    @Test
    public void testDetectChanges_BothNull() {
        List<RouteChangeEvent> events = detector.detectChanges("test-topic", null, null);
        assertTrue(events.isEmpty());
    }

    @Test
    public void testDetectChanges_BothEmpty() {
        TopicRouteData empty1 = new TopicRouteData();
        empty1.setBrokerDatas(new ArrayList<>());
        empty1.setQueueDatas(new ArrayList<>());

        TopicRouteData empty2 = new TopicRouteData();
        empty2.setBrokerDatas(new ArrayList<>());
        empty2.setQueueDatas(new ArrayList<>());

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", empty1, empty2);
        assertTrue(events.isEmpty());
    }

    @Test
    public void testDetectChanges_SameRoute() {
        TopicRouteData route = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", route, route);
        assertTrue(events.isEmpty());
    }

    // ==================== Broker Online Tests ====================

    @Test
    public void testDetectChanges_BrokerOnline_NewBrokerName() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{
                createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
                createBrokerData("cluster-a", "broker-1", 0L, "127.0.0.2:10911")
            },
            new QueueData[]{
                createQueueData("broker-0", 4, 4, 6),
                createQueueData("broker-1", 4, 4, 6)
            }
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasBrokerOnline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_ONLINE
                && "broker-1".equals(e.getBrokerName()));
        assertTrue("Should detect broker-1 online", hasBrokerOnline);
    }

    @Test
    public void testDetectChanges_BrokerOnline_NewBrokerInstance() {
        // Old route: broker-0 with master (id=0)
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        // New route: broker-0 with master (id=0) and slave (id=1)
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        addrs.put(1L, "127.0.0.1:10921");
        BrokerData newBrokerData = new BrokerData();
        newBrokerData.setCluster("cluster-a");
        newBrokerData.setBrokerName("broker-0");
        newBrokerData.setBrokerAddrs(addrs);

        TopicRouteData newRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{newBrokerData},
            new QueueData[]{createQueueData("broker-0", 4, 4, 6)}
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasBrokerOnline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_ONLINE
                && e.getBrokerId() == 1L);
        assertTrue("Should detect new slave broker instance online", hasBrokerOnline);
    }

    // ==================== Broker Offline Tests ====================

    @Test
    public void testDetectChanges_BrokerOffline_RemovedBrokerName() {
        TopicRouteData oldRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{
                createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
                createBrokerData("cluster-a", "broker-1", 0L, "127.0.0.2:10911")
            },
            new QueueData[]{
                createQueueData("broker-0", 4, 4, 6),
                createQueueData("broker-1", 4, 4, 6)
            }
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasBrokerOffline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_OFFLINE
                && "broker-1".equals(e.getBrokerName()));
        assertTrue("Should detect broker-1 offline", hasBrokerOffline);
    }

    @Test
    public void testDetectChanges_BrokerOffline_RemovedBrokerInstance() {
        HashMap<Long, String> oldAddrs = new HashMap<>();
        oldAddrs.put(0L, "127.0.0.1:10911");
        oldAddrs.put(1L, "127.0.0.1:10921");
        BrokerData oldBrokerData = new BrokerData();
        oldBrokerData.setCluster("cluster-a");
        oldBrokerData.setBrokerName("broker-0");
        oldBrokerData.setBrokerAddrs(oldAddrs);

        TopicRouteData oldRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{oldBrokerData},
            new QueueData[]{createQueueData("broker-0", 4, 4, 6)}
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasBrokerOffline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_OFFLINE
                && e.getBrokerId() == 1L);
        assertTrue("Should detect slave broker instance offline", hasBrokerOffline);
    }

    // ==================== Broker Address Change Tests ====================

    @Test
    public void testDetectChanges_BrokerAddressChange() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10912"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasOffline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_OFFLINE
                && "127.0.0.1:10911".equals(e.getBrokerAddress()));
        boolean hasOnline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_ONLINE
                && "127.0.0.1:10912".equals(e.getBrokerAddress()));
        assertTrue("Should detect broker offline for old address", hasOffline);
        assertTrue("Should detect broker online for new address", hasOnline);
    }

    // ==================== Queue Scale Tests ====================

    @Test
    public void testDetectChanges_QueueScale_ReadQueueChange() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 8, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasQueueScale = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.QUEUE_SCALE
                && e.getPreviousReadQueueNums() == 4
                && e.getCurrentReadQueueNums() == 8);
        assertTrue("Should detect read queue scale change", hasQueueScale);
    }

    @Test
    public void testDetectChanges_QueueScale_WriteQueueChange() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 8, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        boolean hasQueueScale = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.QUEUE_SCALE
                && e.getPreviousWriteQueueNums() == 4
                && e.getCurrentWriteQueueNums() == 8);
        assertTrue("Should detect write queue scale change", hasQueueScale);
    }

    @Test
    public void testDetectChanges_NoQueueScaleWhenSame() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);
        assertTrue("No events when route is the same", events.isEmpty());
    }

    // ==================== Complex Scenario Tests ====================

    @Test
    public void testDetectChanges_MultipleBrokerAndQueueChanges() {
        TopicRouteData oldRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{
                createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
                createBrokerData("cluster-a", "broker-1", 0L, "127.0.0.2:10911")
            },
            new QueueData[]{
                createQueueData("broker-0", 4, 4, 6),
                createQueueData("broker-1", 4, 4, 6)
            }
        );

        TopicRouteData newRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{
                createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
                createBrokerData("cluster-a", "broker-2", 0L, "127.0.0.3:10911")
            },
            new QueueData[]{
                createQueueData("broker-0", 8, 4, 6),
                createQueueData("broker-2", 4, 4, 6)
            }
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        // broker-1 offline, broker-2 online, broker-0 queue scale
        boolean hasBroker1Offline = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_OFFLINE
                && "broker-1".equals(e.getBrokerName()));
        boolean hasBroker2Online = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.BROKER_ONLINE
                && "broker-2".equals(e.getBrokerName()));
        boolean hasQueueScale = events.stream()
            .anyMatch(e -> e.getEventType() == RouteChangeEventType.QUEUE_SCALE
                && "broker-0".equals(e.getBrokerName()));

        assertTrue("Should detect broker-1 offline", hasBroker1Offline);
        assertTrue("Should detect broker-2 online", hasBroker2Online);
        assertTrue("Should detect broker-0 queue scale", hasQueueScale);
    }

    @Test
    public void testDetectChanges_TopicCreateWithMultipleBrokers() {
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        addrs.put(1L, "127.0.0.1:10921");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("cluster-a");
        brokerData.setBrokerName("broker-0");
        brokerData.setBrokerAddrs(addrs);

        TopicRouteData newRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{brokerData},
            new QueueData[]{createQueueData("broker-0", 8, 8, 6)}
        );

        List<RouteChangeEvent> events = detector.detectChanges("multi-broker-topic", null, newRoute);

        // 1 TOPIC_CREATE + 2 BROKER_ONLINE (master + slave)
        assertEquals(3, events.size());
        assertEquals(RouteChangeEventType.TOPIC_CREATE, events.get(0).getEventType());
        long brokerOnlineCount = events.stream()
            .filter(e -> e.getEventType() == RouteChangeEventType.BROKER_ONLINE)
            .count();
        assertEquals(2, brokerOnlineCount);
    }

    // ==================== Event Field Tests ====================

    @Test
    public void testDetectChanges_EventFieldsPopulated() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 8, 8, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("field-test", oldRoute, newRoute);

        RouteChangeEvent queueEvent = events.stream()
            .filter(e -> e.getEventType() == RouteChangeEventType.QUEUE_SCALE)
            .findFirst()
            .orElse(null);

        assertNotNull(queueEvent);
        assertEquals("field-test", queueEvent.getTopic());
        assertEquals("broker-0", queueEvent.getBrokerName());
        assertEquals(4, queueEvent.getPreviousReadQueueNums());
        assertEquals(8, queueEvent.getCurrentReadQueueNums());
        assertEquals(4, queueEvent.getPreviousWriteQueueNums());
        assertEquals(8, queueEvent.getCurrentWriteQueueNums());
        assertTrue(queueEvent.getTimestamp() > 0);
    }

    @Test
    public void testDetectChanges_BrokerEventFieldsPopulated() {
        TopicRouteData oldRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteData newRoute = createRouteDataWithMultipleBrokers(
            new BrokerData[]{
                createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
                createBrokerData("cluster-b", "broker-1", 0L, "127.0.0.2:10911")
            },
            new QueueData[]{
                createQueueData("broker-0", 4, 4, 6),
                createQueueData("broker-1", 4, 4, 6)
            }
        );

        List<RouteChangeEvent> events = detector.detectChanges("test-topic", oldRoute, newRoute);

        RouteChangeEvent brokerEvent = events.stream()
            .filter(e -> e.getEventType() == RouteChangeEventType.BROKER_ONLINE
                && "broker-1".equals(e.getBrokerName()))
            .findFirst()
            .orElse(null);

        assertNotNull(brokerEvent);
        assertEquals("cluster-b", brokerEvent.getCluster());
        assertEquals(0L, brokerEvent.getBrokerId());
        assertEquals("127.0.0.2:10911", brokerEvent.getBrokerAddress());
    }

    // ==================== Snapshot Building Tests ====================

    @Test
    public void testBuildSnapshot_NormalRoute() {
        TopicRouteData route = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        TopicRouteSnapshot snapshot = detector.buildSnapshot("test-topic", route);

        assertEquals("test-topic", snapshot.getTopic());
        assertEquals(1, snapshot.getBrokers().size());
        assertEquals("cluster-a", snapshot.getBrokers().get(0).getCluster());
        assertEquals("broker-0", snapshot.getBrokers().get(0).getBrokerName());
        assertEquals(1, snapshot.getBrokers().get(0).getBrokerAddrs().size());
        assertEquals("127.0.0.1:10911", snapshot.getBrokers().get(0).getBrokerAddrs().get(0L));
        assertEquals(1, snapshot.getQueues().size());
        assertEquals("broker-0", snapshot.getQueues().get(0).getBrokerName());
        assertEquals(4, snapshot.getQueues().get(0).getReadQueueNums());
        assertEquals(4, snapshot.getQueues().get(0).getWriteQueueNums());
        assertEquals(6, snapshot.getQueues().get(0).getPerm());
    }

    @Test
    public void testBuildSnapshot_NullRoute() {
        TopicRouteSnapshot snapshot = detector.buildSnapshot("test-topic", null);

        assertEquals("test-topic", snapshot.getTopic());
        assertNotNull(snapshot.getBrokers());
        assertTrue(snapshot.getBrokers().isEmpty());
        assertNotNull(snapshot.getQueues());
        assertTrue(snapshot.getQueues().isEmpty());
    }

    @Test
    public void testBuildSnapshot_MultipleBrokers() {
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, "127.0.0.1:10911");
        addrs.put(1L, "127.0.0.1:10921");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("cluster-a");
        brokerData.setBrokerName("broker-0");
        brokerData.setBrokerAddrs(addrs);

        TopicRouteData route = createRouteDataWithMultipleBrokers(
            new BrokerData[]{brokerData},
            new QueueData[]{createQueueData("broker-0", 8, 8, 6)}
        );

        TopicRouteSnapshot snapshot = detector.buildSnapshot("multi-addr-topic", route);

        assertEquals("multi-addr-topic", snapshot.getTopic());
        assertEquals(1, snapshot.getBrokers().size());
        assertEquals(2, snapshot.getBrokers().get(0).getBrokerAddrs().size());
        assertEquals("127.0.0.1:10911", snapshot.getBrokers().get(0).getBrokerAddrs().get(0L));
        assertEquals("127.0.0.1:10921", snapshot.getBrokers().get(0).getBrokerAddrs().get(1L));
    }

    // ==================== Topic Create with Snapshot ====================

    @Test
    public void testDetectChanges_TopicCreateHasSnapshot() {
        TopicRouteData newRoute = createRouteData(
            createBrokerData("cluster-a", "broker-0", 0L, "127.0.0.1:10911"),
            createQueueData("broker-0", 4, 4, 6)
        );

        List<RouteChangeEvent> events = detector.detectChanges("snapshot-topic", null, newRoute);

        RouteChangeEvent createEvent = events.get(0);
        assertEquals(RouteChangeEventType.TOPIC_CREATE, createEvent.getEventType());
        assertNotNull(createEvent.getRouteSnapshot());
        assertEquals("snapshot-topic", createEvent.getRouteSnapshot().getTopic());
    }

    // ==================== Helper Methods ====================

    private TopicRouteData createRouteData(BrokerData brokerData, QueueData queueData) {
        TopicRouteData routeData = new TopicRouteData();
        List<BrokerData> brokerDatas = new ArrayList<>();
        brokerDatas.add(brokerData);
        List<QueueData> queueDatas = new ArrayList<>();
        queueDatas.add(queueData);
        routeData.setBrokerDatas(brokerDatas);
        routeData.setQueueDatas(queueDatas);
        return routeData;
    }

    private TopicRouteData createRouteDataWithMultipleBrokers(BrokerData[] brokerDatas, QueueData[] queueDatas) {
        TopicRouteData routeData = new TopicRouteData();
        List<BrokerData> brokerList = new ArrayList<>();
        for (BrokerData bd : brokerDatas) {
            brokerList.add(bd);
        }
        List<QueueData> queueList = new ArrayList<>();
        for (QueueData qd : queueDatas) {
            queueList.add(qd);
        }
        routeData.setBrokerDatas(brokerList);
        routeData.setQueueDatas(queueList);
        return routeData;
    }

    private BrokerData createBrokerData(String cluster, String brokerName, long brokerId, String brokerAddr) {
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster(cluster);
        brokerData.setBrokerName(brokerName);
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(brokerId, brokerAddr);
        brokerData.setBrokerAddrs(addrs);
        return brokerData;
    }

    private QueueData createQueueData(String brokerName, int readQueueNums, int writeQueueNums, int perm) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setReadQueueNums(readQueueNums);
        queueData.setWriteQueueNums(writeQueueNums);
        queueData.setPerm(perm);
        return queueData;
    }
}