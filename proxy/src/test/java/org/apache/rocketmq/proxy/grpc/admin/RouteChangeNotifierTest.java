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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.RouteChangeEvent;
import apache.rocketmq.v2.RouteChangeEventType;
import apache.rocketmq.v2.SubscribeRouteEventsRequest;
import apache.rocketmq.v2.SubscribeRouteEventsResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RouteChangeNotifierTest {

    private RouteChangeNotifier notifier;
    private RecordingObserver observer;

    private static class RecordingObserver implements StreamObserver<SubscribeRouteEventsResponse> {
        final List<RouteChangeEvent> events = Collections.synchronizedList(new ArrayList<>());
        volatile boolean completed;

        @Override
        public void onNext(SubscribeRouteEventsResponse value) {
            events.add(value.getEvent());
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    @Before
    public void setUp() {
        notifier = new RouteChangeNotifier();
        observer = new RecordingObserver();
    }

    private static MessageQueueView view(String topic, String brokerName, String brokerAddr, int readQueues,
        int writeQueues) {
        TopicRouteData data = new TopicRouteData();
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("DefaultCluster");
        brokerData.setBrokerName(brokerName);
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(0L, brokerAddr);
        brokerData.setBrokerAddrs(addrs);
        data.setBrokerDatas(Collections.singletonList(brokerData));
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setReadQueueNums(readQueues);
        queueData.setWriteQueueNums(writeQueues);
        queueData.setPerm(6);
        data.setQueueDatas(Collections.singletonList(queueData));
        return new MessageQueueView(topic, data, null);
    }

    private static MessageQueueView multiBrokerView(String topic) {
        TopicRouteData data = new TopicRouteData();
        List<BrokerData> brokers = new ArrayList<>();
        List<QueueData> queues = new ArrayList<>();
        String[] names = {"broker-a", "broker-b"};
        for (int i = 0; i < names.length; i++) {
            BrokerData brokerData = new BrokerData();
            brokerData.setCluster("DefaultCluster");
            brokerData.setBrokerName(names[i]);
            HashMap<Long, String> addrs = new HashMap<>();
            addrs.put(0L, "127.0.0.1:1091" + i);
            brokerData.setBrokerAddrs(addrs);
            brokers.add(brokerData);
            QueueData queueData = new QueueData();
            queueData.setBrokerName(names[i]);
            queueData.setReadQueueNums(i == 0 ? 8 : 4);
            queueData.setWriteQueueNums(4);
            queueData.setPerm(6);
            queues.add(queueData);
        }
        data.setBrokerDatas(brokers);
        data.setQueueDatas(queues);
        return new MessageQueueView(topic, data, null);
    }

    @Test
    public void subscribeReplaysSnapshotAndDetectsChanges() {
        MessageQueueView initial = view("TopicTest", "broker-a", "127.0.0.1:10911", 4, 4);
        notifier.onRouteLoaded("TopicTest", initial);

        notifier.subscribe(SubscribeRouteEventsRequest.newBuilder().build(), observer,
            Collections.singletonMap("TopicTest", initial));
        assertEquals(1, observer.events.size());
        assertEquals(RouteChangeEventType.ROUTE_SNAPSHOT, observer.events.get(0).getEventType());
        assertEquals("TopicTest", observer.events.get(0).getTopic());
        assertEquals("broker-a", observer.events.get(0).getRouteSnapshot().getBrokers(0).getBrokerName());

        // scale queues and add a second broker
        notifier.onRouteRefreshed("TopicTest", view("TopicTest", "broker-a", "127.0.0.1:10911", 4, 4),
            multiBrokerView("TopicTest"));

        List<RouteChangeEventType> types = new ArrayList<>();
        for (int i = 1; i < observer.events.size(); i++) {
            types.add(observer.events.get(i).getEventType());
        }
        assertTrue("expected BROKER_ONLINE in " + types, types.contains(RouteChangeEventType.BROKER_ONLINE));
        assertTrue("expected QUEUE_SCALE in " + types, types.contains(RouteChangeEventType.QUEUE_SCALE));
    }

    @Test
    public void topicDeleteDetectedWhenRouteDisappears() {
        notifier.onRouteLoaded("TopicGone", view("TopicGone", "broker-a", "127.0.0.1:10911", 4, 4));
        notifier.subscribe(SubscribeRouteEventsRequest.newBuilder().build(), observer,
            Collections.emptyMap());

        notifier.onRouteRefreshed("TopicGone", view("TopicGone", "broker-a", "127.0.0.1:10911", 4, 4),
            MessageQueueView.WRAPPED_EMPTY_QUEUE);

        boolean deleted = false;
        for (RouteChangeEvent event : observer.events) {
            if (event.getEventType() == RouteChangeEventType.TOPIC_DELETE) {
                deleted = true;
            }
        }
        assertTrue(deleted);
    }

    @Test
    public void topicFilterAppliesToSubscribers() {
        notifier.subscribe(SubscribeRouteEventsRequest.newBuilder().addTopics("Other").build(), observer,
            Collections.emptyMap());

        notifier.onRouteLoaded("TopicTest", view("TopicTest", "broker-a", "127.0.0.1:10911", 4, 4));
        assertEquals(0, observer.events.size());

        notifier.onRouteLoaded("Other", view("Other", "broker-a", "127.0.0.1:10911", 4, 4));
        assertEquals(1, observer.events.size());
        assertEquals("Other", observer.events.get(0).getTopic());
    }

    @Test
    public void unsubscribeStopsDelivery() {
        MessageQueueView initial = view("TopicTest", "broker-a", "127.0.0.1:10911", 4, 4);
        notifier.onRouteLoaded("TopicTest", initial);
        RouteChangeNotifier.Subscription subscription = notifier.subscribe(
            SubscribeRouteEventsRequest.newBuilder().build(), observer,
            Collections.singletonMap("TopicTest", initial));
        assertEquals(1, observer.events.size());
        assertEquals(1, notifier.getSubscriptionCount());

        notifier.unsubscribe(subscription);
        assertEquals(0, notifier.getSubscriptionCount());

        notifier.onRouteRefreshed("TopicTest", view("TopicTest", "broker-a", "127.0.0.1:10911", 4, 4),
            view("TopicTest", "broker-a", "127.0.0.1:10911", 16, 16));
        assertEquals(1, observer.events.size());
    }

    @Test
    public void shutdownCompletesSubscribers() {
        notifier.onRouteLoaded("TopicTest", view("TopicTest", "broker-a", "127.0.0.1:10911", 4, 4));
        notifier.subscribe(SubscribeRouteEventsRequest.newBuilder().build(), observer, Collections.emptyMap());
        try {
            notifier.shutdown();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertTrue(observer.completed);
        assertEquals(0, notifier.getSubscriptionCount());
    }
}
