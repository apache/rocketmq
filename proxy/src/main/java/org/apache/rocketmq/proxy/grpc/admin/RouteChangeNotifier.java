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

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEvent;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsResponse;

/**
 * Manages route change event subscriptions and broadcasts events to subscribers.
 * <p>
 * Integrates with {@link TopicRouteService} Caffeine cache refresh mechanism:
 * when the cache reloads route data (every 20s by default), this notifier
 * compares old and new route data using {@link RouteChangeEventDetector}
 * and pushes detected changes to all active subscribers.
 * <p>
 * Subscription management:
 * - Subscribers are tracked in a CopyOnWriteArrayList for thread safety
 * - Each subscriber can filter by topics and event types
 * - Client disconnection is handled via ServerCallStreamObserver.setOnCancelHandler
 * - Initial route snapshot is sent on subscription for immediate state sync
 */
public class RouteChangeNotifier implements TopicRouteService.RouteRefreshListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final RouteChangeEventDetector detector;
    private final CopyOnWriteArrayList<RouteEventSubscription> subscriptions;
    private final TopicRouteService topicRouteService;

    public RouteChangeNotifier(TopicRouteService topicRouteService) {
        this.topicRouteService = topicRouteService;
        this.detector = new RouteChangeEventDetector();
        this.subscriptions = new CopyOnWriteArrayList<>();
    }

    /**
     * Subscribe to route change events.
     * Sends an initial route snapshot for all subscribed topics, then pushes
     * incremental changes as they are detected.
     *
     * @param topics filter: only receive events for these topics (empty = all topics)
     * @param eventTypes filter: only receive these event types (empty = all types)
     * @param responseObserver the gRPC stream observer for pushing events
     */
    public void subscribe(List<String> topics, List<RouteChangeEventType> eventTypes,
        StreamObserver<SubscribeRouteEventsResponse> responseObserver) {
        RouteEventSubscription subscription = new RouteEventSubscription(topics, eventTypes, responseObserver);

        // Register cancellation handler for client disconnect
        if (responseObserver instanceof ServerCallStreamObserver) {
            ServerCallStreamObserver<SubscribeRouteEventsResponse> serverObserver =
                (ServerCallStreamObserver<SubscribeRouteEventsResponse>) responseObserver;
            serverObserver.setOnCancelHandler(() -> {
                subscriptions.remove(subscription);
                log.info("Route event subscriber disconnected. Active subscribers: {}", subscriptions.size());
            });
        }

        subscriptions.add(subscription);
        log.info("New route event subscriber registered. Topics filter: {}, Event types filter: {}, Active subscribers: {}",
            topics, eventTypes, subscriptions.size());

        // Send initial route snapshot for all cached topics
        try {
            sendInitialSnapshot(subscription);
        } catch (Exception e) {
            log.error("Failed to send initial route snapshot to new subscriber", e);
        }
    }

    /**
     * Called by TopicRouteService cache reload listener when route data changes.
     * Compares old and new route data, detects changes, and broadcasts to subscribers.
     *
     * @param topic the topic whose route was refreshed
     * @param oldView the previous MessageQueueView (may be null)
     * @param newView the new MessageQueueView (may be null/empty)
     */
    public void onRouteRefreshed(String topic, MessageQueueView oldView, MessageQueueView newView) {
        if (subscriptions.isEmpty()) {
            return;
        }

        TopicRouteData oldRoute = oldView != null ? oldView.getTopicRouteData() : null;
        TopicRouteData newRoute = newView != null ? newView.getTopicRouteData() : null;

        List<RouteChangeEvent> events = detector.detectChanges(topic, oldRoute, newRoute);
        if (events.isEmpty()) {
            return;
        }

        log.debug("Detected {} route change events for topic {}", events.size(), topic);
        broadcastEvents(events);
    }

    /**
     * Broadcast detected events to all matching subscribers.
     */
    private void broadcastEvents(List<RouteChangeEvent> events) {
        for (RouteChangeEvent event : events) {
            for (RouteEventSubscription subscription : subscriptions) {
                if (!subscription.matches(event)) {
                    continue;
                }
                try {
                    SubscribeRouteEventsResponse response = ProxyAdminProtoConverter.toSubscribeRouteEventsResponse(event);
                    subscription.getResponseObserver().onNext(response);
                } catch (Exception e) {
                    log.warn("Failed to push route event to subscriber, removing. Error: {}", e.getMessage());
                    subscriptions.remove(subscription);
                }
            }
        }
    }

    /**
     * Send initial route snapshot to a newly subscribed client.
     * Iterates all cached topics and sends ROUTE_SNAPSHOT events.
     */
    private void sendInitialSnapshot(RouteEventSubscription subscription) {
        if (topicRouteService == null) {
            return;
        }

        // Get all cached topic names
        Set<String> cachedTopics = topicRouteService.getAllTopicNames();
        Set<String> topicFilter = subscription.getTopicFilter();

        for (String topic : cachedTopics) {
            // Apply topic filter
            if (!topicFilter.isEmpty() && !topicFilter.contains(topic)) {
                continue;
            }

            try {
                MessageQueueView view = topicRouteService.getCachedTopicRouteData(topic);
                if (view == null) {
                    continue;
                }

                RouteChangeEvent snapshotEvent = new RouteChangeEvent();
                snapshotEvent.setEventType(RouteChangeEventType.ROUTE_SNAPSHOT);
                snapshotEvent.setTimestamp(System.currentTimeMillis());
                snapshotEvent.setTopic(topic);
                snapshotEvent.setRouteSnapshot(detector.buildSnapshot(topic, view.getTopicRouteData()));

                // Check event type filter
                if (!subscription.matchesEventType(snapshotEvent.getEventType())) {
                    continue;
                }

                SubscribeRouteEventsResponse response = ProxyAdminProtoConverter.toSubscribeRouteEventsResponse(snapshotEvent);
                subscription.getResponseObserver().onNext(response);
            } catch (Exception e) {
                log.warn("Failed to send initial snapshot for topic {}: {}", topic, e.getMessage());
            }
        }
    }

    /**
     * Get the number of active subscribers.
     */
    public int getSubscriberCount() {
        return subscriptions.size();
    }

    /**
     * Shutdown the notifier and close all subscriber streams.
     */
    public void shutdown() {
        for (RouteEventSubscription subscription : subscriptions) {
            try {
                subscription.getResponseObserver().onCompleted();
            } catch (Exception e) {
                log.debug("Error closing subscriber stream during shutdown", e);
            }
        }
        subscriptions.clear();
        log.info("RouteChangeNotifier shutdown complete");
    }

    /**
     * Internal representation of a route event subscription.
     */
    private static class RouteEventSubscription {
        private final Set<String> topicFilter;
        private final Set<RouteChangeEventType> eventTypeFilter;
        private final StreamObserver<SubscribeRouteEventsResponse> responseObserver;

        RouteEventSubscription(List<String> topics, List<RouteChangeEventType> eventTypes,
            StreamObserver<SubscribeRouteEventsResponse> responseObserver) {
            this.topicFilter = topics != null && !topics.isEmpty() ? new HashSet<>(topics) : new HashSet<>();
            this.eventTypeFilter = eventTypes != null && !eventTypes.isEmpty() ? new HashSet<>(eventTypes) : new HashSet<>();
            this.responseObserver = responseObserver;
        }

        Set<String> getTopicFilter() {
            return topicFilter;
        }

        StreamObserver<SubscribeRouteEventsResponse> getResponseObserver() {
            return responseObserver;
        }

        boolean matchesEventType(RouteChangeEventType eventType) {
            return eventTypeFilter.isEmpty() || eventTypeFilter.contains(eventType);
        }

        boolean matches(RouteChangeEvent event) {
            // Check topic filter
            if (!topicFilter.isEmpty() && !topicFilter.contains(event.getTopic())) {
                return false;
            }
            // Check event type filter
            return matchesEventType(event.getEventType());
        }
    }
}