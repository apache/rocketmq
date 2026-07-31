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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RouteChangeEventTest {

    @Test
    public void testDefaultConstructor() {
        RouteChangeEvent event = new RouteChangeEvent();
        assertNull(event.getEventType());
        assertEquals(0, event.getTimestamp());
        assertNull(event.getTopic());
        assertNull(event.getCluster());
        assertNull(event.getBrokerName());
        assertEquals(0, event.getBrokerId());
        assertNull(event.getBrokerAddress());
        assertEquals(0, event.getPreviousReadQueueNums());
        assertEquals(0, event.getCurrentReadQueueNums());
        assertEquals(0, event.getPreviousWriteQueueNums());
        assertEquals(0, event.getCurrentWriteQueueNums());
        assertNull(event.getRouteSnapshot());
    }

    @Test
    public void testSettersAndGetters() {
        RouteChangeEvent event = new RouteChangeEvent();
        TopicRouteSnapshot snapshot = new TopicRouteSnapshot();
        snapshot.setTopic("test-topic");

        event.setEventType(RouteChangeEventType.BROKER_ONLINE);
        event.setTimestamp(1234567890L);
        event.setTopic("test-topic");
        event.setCluster("test-cluster");
        event.setBrokerName("broker-a");
        event.setBrokerId(0L);
        event.setBrokerAddress("127.0.0.1:10911");
        event.setPreviousReadQueueNums(4);
        event.setCurrentReadQueueNums(8);
        event.setPreviousWriteQueueNums(4);
        event.setCurrentWriteQueueNums(8);
        event.setRouteSnapshot(snapshot);

        assertEquals(RouteChangeEventType.BROKER_ONLINE, event.getEventType());
        assertEquals(1234567890L, event.getTimestamp());
        assertEquals("test-topic", event.getTopic());
        assertEquals("test-cluster", event.getCluster());
        assertEquals("broker-a", event.getBrokerName());
        assertEquals(0L, event.getBrokerId());
        assertEquals("127.0.0.1:10911", event.getBrokerAddress());
        assertEquals(4, event.getPreviousReadQueueNums());
        assertEquals(8, event.getCurrentReadQueueNums());
        assertEquals(4, event.getPreviousWriteQueueNums());
        assertEquals(8, event.getCurrentWriteQueueNums());
        assertEquals(snapshot, event.getRouteSnapshot());
    }

    @Test
    public void testAllEventTypes() {
        RouteChangeEvent event = new RouteChangeEvent();

        for (RouteChangeEventType type : RouteChangeEventType.values()) {
            event.setEventType(type);
            assertEquals(type, event.getEventType());
        }
    }

    @Test
    public void testQueueScaleEvent() {
        RouteChangeEvent event = new RouteChangeEvent();
        event.setEventType(RouteChangeEventType.QUEUE_SCALE);
        event.setTopic("scale-topic");
        event.setBrokerName("broker-b");
        event.setPreviousReadQueueNums(4);
        event.setCurrentReadQueueNums(8);
        event.setPreviousWriteQueueNums(4);
        event.setCurrentWriteQueueNums(8);

        assertEquals(RouteChangeEventType.QUEUE_SCALE, event.getEventType());
        assertEquals(4, event.getPreviousReadQueueNums());
        assertEquals(8, event.getCurrentReadQueueNums());
        assertEquals(4, event.getPreviousWriteQueueNums());
        assertEquals(8, event.getCurrentWriteQueueNums());
    }

    @Test
    public void testTopicCreateEvent() {
        RouteChangeEvent event = new RouteChangeEvent();
        event.setEventType(RouteChangeEventType.TOPIC_CREATE);
        event.setTopic("new-topic");

        assertEquals(RouteChangeEventType.TOPIC_CREATE, event.getEventType());
        assertEquals("new-topic", event.getTopic());
    }

    @Test
    public void testTopicDeleteEvent() {
        RouteChangeEvent event = new RouteChangeEvent();
        event.setEventType(RouteChangeEventType.TOPIC_DELETE);
        event.setTopic("deleted-topic");

        assertEquals(RouteChangeEventType.TOPIC_DELETE, event.getEventType());
        assertEquals("deleted-topic", event.getTopic());
    }

    @Test
    public void testRouteSnapshotEvent() {
        RouteChangeEvent event = new RouteChangeEvent();
        event.setEventType(RouteChangeEventType.ROUTE_SNAPSHOT);
        event.setTopic("snapshot-topic");

        TopicRouteSnapshot snapshot = new TopicRouteSnapshot();
        snapshot.setTopic("snapshot-topic");
        event.setRouteSnapshot(snapshot);

        assertEquals(RouteChangeEventType.ROUTE_SNAPSHOT, event.getEventType());
        assertEquals("snapshot-topic", event.getRouteSnapshot().getTopic());
    }
}