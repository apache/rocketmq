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

import apache.rocketmq.v2.BrokerInfo;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.QueueInfo;
import apache.rocketmq.v2.RouteChangeEvent;
import apache.rocketmq.v2.RouteChangeEventType;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SubscribeRouteEventsRequest;
import apache.rocketmq.v2.SubscribeRouteEventsResponse;
import apache.rocketmq.v2.TopicRouteSnapshot;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;

/**
 * RIP-2 route observation: detects route changes from the proxy's topic route
 * cache refreshes and streams them to admin subscribers (SubscribeRouteEvents).
 *
 * <p>This class is protocol-pure: it only works on the RIP-2 gRPC contract
 * ({@code apache.rocketmq.v2.*} generated from rocketmq-apis). The translation
 * from the broker-internal route representation into the v2 proto snapshot is
 * delegated to {@link AdminModelConverter}.
 *
 * <p>Detected event types:
 * <ul>
 *   <li>{@code ROUTE_SNAPSHOT} — full route snapshot emitted on first observation
 *       of a topic and immediately on subscribe (replay of current state);</li>
 *   <li>{@code TOPIC_CREATE} / {@code TOPIC_DELETE} — route appears / disappears;</li>
 *   <li>{@code QUEUE_SCALE} — read/write queue nums changed on a broker;</li>
 *   <li>{@code BROKER_ONLINE} / {@code BROKER_OFFLINE} — broker added/removed
 *       from the route (registration change on the NameServer).</li>
 * </ul>
 */
public class RouteChangeNotifier implements TopicRouteService.RouteRefreshListener, StartAndShutdown {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * A live SubscribeRouteEvents stream with its filters.
     */
    public final class Subscription {
        private final Set<String> topics;
        private final Set<RouteChangeEventType> eventTypes;
        private final StreamObserver<SubscribeRouteEventsResponse> observer;
        private volatile boolean cancelled;

        private Subscription(Set<String> topics, Set<RouteChangeEventType> eventTypes,
            StreamObserver<SubscribeRouteEventsResponse> observer) {
            this.topics = topics;
            this.eventTypes = eventTypes;
            this.observer = observer;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        void cancel() {
            this.cancelled = true;
        }

        private boolean matches(RouteChangeEvent event) {
            if (!topics.isEmpty() && !topics.contains(event.getTopic())) {
                return false;
            }
            return eventTypes.isEmpty() || eventTypes.contains(event.getEventType());
        }

        synchronized void deliver(RouteChangeEvent event) {
            if (cancelled) {
                return;
            }
            try {
                observer.onNext(SubscribeRouteEventsResponse.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .setEvent(event)
                    .build());
            } catch (Throwable t) {
                cancelled = true;
                subscriptions.remove(this);
                log.info("RIP-2 route event subscriber dropped. cause:{}", t.getMessage());
            }
        }
    }

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
    /**
     * Baseline proto snapshots per topic used to diff refreshes into change events.
     */
    private final ConcurrentMap<String, TopicRouteSnapshot> baseline = new ConcurrentHashMap<>();

    /**
     * Subscribe to route events. Immediately replays ROUTE_SNAPSHOT events for the topics
     * currently known (matching the request filter), then streams live changes.
     */
    public Subscription subscribe(SubscribeRouteEventsRequest request,
        StreamObserver<SubscribeRouteEventsResponse> observer, Map<String, MessageQueueView> currentRoutes) {
        Set<String> topics = new HashSet<>(request.getTopicsList());
        Set<RouteChangeEventType> types = new HashSet<>(request.getEventTypesList());
        Subscription subscription = new Subscription(topics, types, observer);
        subscriptions.add(subscription);
        // replay current state as ROUTE_SNAPSHOT so the consumer starts from a consistent view
        for (Map.Entry<String, MessageQueueView> entry : currentRoutes.entrySet()) {
            TopicRouteSnapshot snapshot = snapshotOf(entry.getKey(), entry.getValue());
            if (snapshot == null) {
                continue;
            }
            RouteChangeEvent snapshotEvent = baseEvent(RouteChangeEventType.ROUTE_SNAPSHOT, snapshot)
                .setRouteSnapshot(snapshot)
                .build();
            if (subscription.matches(snapshotEvent)) {
                subscription.deliver(snapshotEvent);
            }
        }
        return subscription;
    }

    public void unsubscribe(Subscription subscription) {
        if (subscription != null) {
            subscription.cancel();
            subscriptions.remove(subscription);
        }
    }

    @Override
    public void onRouteLoaded(String topic, MessageQueueView view) {
        TopicRouteSnapshot snapshot = snapshotOf(topic, view);
        TopicRouteSnapshot previous = snapshot == null ? baseline.remove(topic) : baseline.put(topic, snapshot);
        if (snapshot == null) {
            if (previous != null) {
                publish(baseEvent(RouteChangeEventType.TOPIC_DELETE, previous).build());
            }
            return;
        }
        if (previous == null) {
            // first observation: emit a snapshot so subscribers learn the current state
            publish(baseEvent(RouteChangeEventType.ROUTE_SNAPSHOT, snapshot)
                .setRouteSnapshot(snapshot)
                .build());
        } else {
            diffAndPublish(previous, snapshot);
        }
    }

    @Override
    public void onRouteRefreshed(String topic, MessageQueueView oldView, MessageQueueView newView) {
        TopicRouteSnapshot oldSnapshot = snapshotOf(topic, oldView);
        if (oldSnapshot == null) {
            oldSnapshot = baseline.get(topic);
        }
        TopicRouteSnapshot newSnapshot = snapshotOf(topic, newView);
        if (newSnapshot == null) {
            baseline.remove(topic);
            if (oldSnapshot != null) {
                publish(baseEvent(RouteChangeEventType.TOPIC_DELETE, oldSnapshot).build());
            }
            return;
        }
        baseline.put(topic, newSnapshot);
        if (oldSnapshot == null) {
            publish(baseEvent(RouteChangeEventType.TOPIC_CREATE, newSnapshot)
                .setRouteSnapshot(newSnapshot)
                .build());
            return;
        }
        diffAndPublish(oldSnapshot, newSnapshot);
    }

    private void diffAndPublish(TopicRouteSnapshot oldSnapshot, TopicRouteSnapshot newSnapshot) {
        String topic = newSnapshot.getTopic();

        Map<String, BrokerInfo> oldBrokers = brokersByName(oldSnapshot);
        Map<String, BrokerInfo> newBrokers = brokersByName(newSnapshot);
        for (Map.Entry<String, BrokerInfo> entry : newBrokers.entrySet()) {
            if (!oldBrokers.containsKey(entry.getKey())) {
                publish(baseEvent(RouteChangeEventType.BROKER_ONLINE, newSnapshot)
                    .setBrokerName(entry.getKey())
                    .setBrokerAddress(masterAddr(entry.getValue()))
                    .build());
            }
        }
        for (Map.Entry<String, BrokerInfo> entry : oldBrokers.entrySet()) {
            if (!newBrokers.containsKey(entry.getKey())) {
                publish(baseEvent(RouteChangeEventType.BROKER_OFFLINE, newSnapshot)
                    .setBrokerName(entry.getKey())
                    .setBrokerAddress(masterAddr(entry.getValue()))
                    .build());
            }
        }

        Map<String, QueueInfo> oldQueues = queuesByBroker(oldSnapshot);
        Map<String, QueueInfo> newQueues = queuesByBroker(newSnapshot);
        for (Map.Entry<String, QueueInfo> entry : newQueues.entrySet()) {
            QueueInfo oldQueue = oldQueues.get(entry.getKey());
            QueueInfo newQueue = entry.getValue();
            if (oldQueue == null) {
                continue;
            }
            if (oldQueue.getReadQueueNums() != newQueue.getReadQueueNums()
                || oldQueue.getWriteQueueNums() != newQueue.getWriteQueueNums()) {
                publish(baseEvent(RouteChangeEventType.QUEUE_SCALE, newSnapshot)
                    .setBrokerName(entry.getKey())
                    .setPreviousReadQueueNums(oldQueue.getReadQueueNums())
                    .setCurrentReadQueueNums(newQueue.getReadQueueNums())
                    .setPreviousWriteQueueNums(oldQueue.getWriteQueueNums())
                    .setCurrentWriteQueueNums(newQueue.getWriteQueueNums())
                    .build());
            }
        }
    }

    /**
     * Convert the proxy-internal route view into the v2 proto snapshot through the shared
     * converter layer (the only place that touches broker-internal route types).
     */
    private static TopicRouteSnapshot snapshotOf(String topic, MessageQueueView view) {
        if (view == null || view.isEmptyCachedQueue()) {
            return null;
        }
        return AdminModelConverter.toTopicRouteSnapshot(topic, view.getTopicRouteData());
    }

    private RouteChangeEvent.Builder baseEvent(RouteChangeEventType type, TopicRouteSnapshot snapshot) {
        long now = System.currentTimeMillis();
        RouteChangeEvent.Builder builder = RouteChangeEvent.newBuilder()
            .setEventType(type)
            .setTimestamp(Timestamp.newBuilder().setSeconds(now / 1000).setNanos((int) ((now % 1000) * 1_000_000)).build())
            .setTopic(snapshot.getTopic());
        if (snapshot.getBrokersCount() > 0 && !snapshot.getBrokers(0).getCluster().isEmpty()) {
            builder.setCluster(snapshot.getBrokers(0).getCluster());
        }
        return builder;
    }

    private void publish(RouteChangeEvent event) {
        for (Subscription subscription : subscriptions) {
            if (subscription.isCancelled()) {
                subscriptions.remove(subscription);
                continue;
            }
            if (subscription.matches(event)) {
                subscription.deliver(event);
            }
        }
    }

    private static Map<String, BrokerInfo> brokersByName(TopicRouteSnapshot snapshot) {
        Map<String, BrokerInfo> map = new ConcurrentHashMap<>();
        for (BrokerInfo broker : snapshot.getBrokersList()) {
            if (!broker.getBrokerName().isEmpty()) {
                map.put(broker.getBrokerName(), broker);
            }
        }
        return map;
    }

    private static Map<String, QueueInfo> queuesByBroker(TopicRouteSnapshot snapshot) {
        Map<String, QueueInfo> map = new ConcurrentHashMap<>();
        for (QueueInfo queue : snapshot.getQueuesList()) {
            if (!queue.getBrokerName().isEmpty()) {
                map.put(queue.getBrokerName(), queue);
            }
        }
        return map;
    }

    /**
     * MixAll.MASTER_ID == 0; kept inline to avoid pulling a broker constant into the
     * protocol-pure layer.
     */
    private static String masterAddr(BrokerInfo broker) {
        if (broker.getBrokerAddrsMap().containsKey(0L)) {
            return broker.getBrokerAddrsMap().get(0L);
        }
        if (broker.getBrokerAddrsCount() > 0) {
            return broker.getBrokerAddrsMap().values().iterator().next();
        }
        return "";
    }

    public int getSubscriptionCount() {
        return subscriptions.size();
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void shutdown() throws Exception {
        List<Subscription> current = new ArrayList<>(subscriptions);
        subscriptions.clear();
        for (Subscription subscription : current) {
            subscription.cancel();
            try {
                subscription.observer.onCompleted();
            } catch (Throwable ignore) {
                // subscriber may already be gone
            }
        }
    }
}
