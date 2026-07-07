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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEvent;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.proxy.grpc.admin.model.TopicRouteSnapshot;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

/**
 * Detects route changes by comparing old and new TopicRouteData.
 * <p>
 * Used by {@link RouteChangeNotifier} during TopicRouteService cache refresh
 * to identify broker online/offline, queue scaling, and topic create/delete events.
 * <p>
 * Detection strategy (Method A from design):
 * - Compare brokerDatas: detect new/removed brokers (BROKER_ONLINE / BROKER_OFFLINE)
 * - Compare queueDatas: detect read/write queue count changes (QUEUE_SCALE)
 * - Detect topic creation: old route was empty, new route has data (TOPIC_CREATE)
 * - Detect topic deletion: old route had data, new route is empty (TOPIC_DELETE)
 */
public class RouteChangeEventDetector {

    /**
     * Detect route changes between old and new TopicRouteData for a given topic.
     *
     * @param topic the topic name
     * @param oldRoute the previous route data (may be null if topic was not cached)
     * @param newRoute the new route data (may be null/empty if topic was deleted)
     * @return list of detected route change events (empty if no changes)
     */
    public List<RouteChangeEvent> detectChanges(String topic, TopicRouteData oldRoute, TopicRouteData newRoute) {
        List<RouteChangeEvent> events = new ArrayList<>();
        long now = System.currentTimeMillis();

        boolean oldEmpty = isRouteEmpty(oldRoute);
        boolean newEmpty = isRouteEmpty(newRoute);

        // Topic creation: was empty, now has data
        if (oldEmpty && !newEmpty) {
            RouteChangeEvent event = new RouteChangeEvent();
            event.setEventType(RouteChangeEventType.TOPIC_CREATE);
            event.setTimestamp(now);
            event.setTopic(topic);
            event.setRouteSnapshot(buildSnapshot(topic, newRoute));
            events.add(event);
            // Also generate broker online events for all brokers in the new route
            events.addAll(detectBrokerOnlineEvents(topic, newRoute, now));
            return events;
        }

        // Topic deletion: had data, now empty
        if (!oldEmpty && newEmpty) {
            RouteChangeEvent event = new RouteChangeEvent();
            event.setEventType(RouteChangeEventType.TOPIC_DELETE);
            event.setTimestamp(now);
            event.setTopic(topic);
            events.add(event);
            // Also generate broker offline events for all brokers in the old route
            events.addAll(detectBrokerOfflineEvents(topic, oldRoute, now));
            return events;
        }

        // Both empty or both null - no changes
        if (oldEmpty && newEmpty) {
            return events;
        }

        // Both have data - compare broker and queue changes
        events.addAll(detectBrokerChanges(topic, oldRoute, newRoute, now));
        events.addAll(detectQueueChanges(topic, oldRoute, newRoute, now));

        return events;
    }

    /**
     * Detect broker online/offline changes.
     */
    private List<RouteChangeEvent> detectBrokerChanges(String topic,
        TopicRouteData oldRoute, TopicRouteData newRoute, long timestamp) {
        List<RouteChangeEvent> events = new ArrayList<>();

        // Build broker address maps: brokerName -> (brokerId -> address)
        Map<String, Map<Long, String>> oldBrokers = buildBrokerMap(oldRoute);
        Map<String, Map<Long, String>> newBrokers = buildBrokerMap(newRoute);

        // Find new brokers (broker online)
        Set<String> oldBrokerNames = oldBrokers.keySet();
        Set<String> newBrokerNames = newBrokers.keySet();

        // New broker names
        for (String brokerName : newBrokerNames) {
            if (!oldBrokerNames.contains(brokerName)) {
                // Entirely new broker cluster
                Map<Long, String> addrs = newBrokers.get(brokerName);
                for (Map.Entry<Long, String> entry : addrs.entrySet()) {
                    RouteChangeEvent event = createBrokerEvent(topic, newRoute, brokerName,
                        entry.getKey(), entry.getValue(), RouteChangeEventType.BROKER_ONLINE, timestamp);
                    events.add(event);
                }
            } else {
                // Same broker name, check for new broker instances (new brokerId)
                Map<Long, String> oldAddrs = oldBrokers.get(brokerName);
                Map<Long, String> newAddrs = newBrokers.get(brokerName);
                for (Map.Entry<Long, String> entry : newAddrs.entrySet()) {
                    if (!oldAddrs.containsKey(entry.getKey())) {
                        RouteChangeEvent event = createBrokerEvent(topic, newRoute, brokerName,
                            entry.getKey(), entry.getValue(), RouteChangeEventType.BROKER_ONLINE, timestamp);
                        events.add(event);
                    } else if (!oldAddrs.get(entry.getKey()).equals(entry.getValue())) {
                        // Address changed - treat as offline + online
                        RouteChangeEvent offlineEvent = createBrokerEvent(topic, oldRoute, brokerName,
                            entry.getKey(), oldAddrs.get(entry.getKey()), RouteChangeEventType.BROKER_OFFLINE, timestamp);
                        events.add(offlineEvent);
                        RouteChangeEvent onlineEvent = createBrokerEvent(topic, newRoute, brokerName,
                            entry.getKey(), entry.getValue(), RouteChangeEventType.BROKER_ONLINE, timestamp);
                        events.add(onlineEvent);
                    }
                }
            }
        }

        // Removed broker names or instances
        for (String brokerName : oldBrokerNames) {
            if (!newBrokerNames.contains(brokerName)) {
                // Entirely removed broker cluster
                Map<Long, String> addrs = oldBrokers.get(brokerName);
                for (Map.Entry<Long, String> entry : addrs.entrySet()) {
                    RouteChangeEvent event = createBrokerEvent(topic, oldRoute, brokerName,
                        entry.getKey(), entry.getValue(), RouteChangeEventType.BROKER_OFFLINE, timestamp);
                    events.add(event);
                }
            } else {
                // Same broker name, check for removed broker instances
                Map<Long, String> oldAddrs = oldBrokers.get(brokerName);
                Map<Long, String> newAddrs = newBrokers.get(brokerName);
                for (Map.Entry<Long, String> entry : oldAddrs.entrySet()) {
                    if (!newAddrs.containsKey(entry.getKey())) {
                        RouteChangeEvent event = createBrokerEvent(topic, oldRoute, brokerName,
                            entry.getKey(), entry.getValue(), RouteChangeEventType.BROKER_OFFLINE, timestamp);
                        events.add(event);
                    }
                }
            }
        }

        return events;
    }

    /**
     * Detect queue count changes (QUEUE_SCALE).
     */
    private List<RouteChangeEvent> detectQueueChanges(String topic,
        TopicRouteData oldRoute, TopicRouteData newRoute, long timestamp) {
        List<RouteChangeEvent> events = new ArrayList<>();

        // Build queue maps: brokerName -> QueueData
        Map<String, QueueData> oldQueues = buildQueueMap(oldRoute);
        Map<String, QueueData> newQueues = buildQueueMap(newRoute);

        Set<String> allBrokerNames = new HashSet<>();
        allBrokerNames.addAll(oldQueues.keySet());
        allBrokerNames.addAll(newQueues.keySet());

        for (String brokerName : allBrokerNames) {
            QueueData oldQ = oldQueues.get(brokerName);
            QueueData newQ = newQueues.get(brokerName);

            int oldRead = oldQ != null ? oldQ.getReadQueueNums() : 0;
            int newRead = newQ != null ? newQ.getReadQueueNums() : 0;
            int oldWrite = oldQ != null ? oldQ.getWriteQueueNums() : 0;
            int newWrite = newQ != null ? newQ.getWriteQueueNums() : 0;

            if (oldRead != newRead || oldWrite != newWrite) {
                RouteChangeEvent event = new RouteChangeEvent();
                event.setEventType(RouteChangeEventType.QUEUE_SCALE);
                event.setTimestamp(timestamp);
                event.setTopic(topic);
                event.setBrokerName(brokerName);
                event.setPreviousReadQueueNums(oldRead);
                event.setCurrentReadQueueNums(newRead);
                event.setPreviousWriteQueueNums(oldWrite);
                event.setCurrentWriteQueueNums(newWrite);
                events.add(event);
            }
        }

        return events;
    }

    /**
     * Generate broker online events for all brokers in a route (used for TOPIC_CREATE).
     */
    private List<RouteChangeEvent> detectBrokerOnlineEvents(String topic, TopicRouteData route, long timestamp) {
        List<RouteChangeEvent> events = new ArrayList<>();
        if (route == null || route.getBrokerDatas() == null) {
            return events;
        }
        for (BrokerData brokerData : route.getBrokerDatas()) {
            if (brokerData.getBrokerAddrs() != null) {
                for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                    RouteChangeEvent event = createBrokerEvent(topic, route,
                        brokerData.getBrokerName(), entry.getKey(), entry.getValue(),
                        RouteChangeEventType.BROKER_ONLINE, timestamp);
                    events.add(event);
                }
            }
        }
        return events;
    }

    /**
     * Generate broker offline events for all brokers in a route (used for TOPIC_DELETE).
     */
    private List<RouteChangeEvent> detectBrokerOfflineEvents(String topic, TopicRouteData route, long timestamp) {
        List<RouteChangeEvent> events = new ArrayList<>();
        if (route == null || route.getBrokerDatas() == null) {
            return events;
        }
        for (BrokerData brokerData : route.getBrokerDatas()) {
            if (brokerData.getBrokerAddrs() != null) {
                for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                    RouteChangeEvent event = createBrokerEvent(topic, route,
                        brokerData.getBrokerName(), entry.getKey(), entry.getValue(),
                        RouteChangeEventType.BROKER_OFFLINE, timestamp);
                    events.add(event);
                }
            }
        }
        return events;
    }

    private RouteChangeEvent createBrokerEvent(String topic, TopicRouteData route,
        String brokerName, long brokerId, String brokerAddress,
        RouteChangeEventType eventType, long timestamp) {
        RouteChangeEvent event = new RouteChangeEvent();
        event.setEventType(eventType);
        event.setTimestamp(timestamp);
        event.setTopic(topic);
        event.setBrokerName(brokerName);
        event.setBrokerId(brokerId);
        event.setBrokerAddress(brokerAddress);
        // Find cluster name from brokerDatas
        if (route != null && route.getBrokerDatas() != null) {
            for (BrokerData bd : route.getBrokerDatas()) {
                if (bd.getBrokerName().equals(brokerName)) {
                    event.setCluster(bd.getCluster());
                    break;
                }
            }
        }
        return event;
    }

    private boolean isRouteEmpty(TopicRouteData route) {
        if (route == null) {
            return true;
        }
        return (route.getBrokerDatas() == null || route.getBrokerDatas().isEmpty())
            && (route.getQueueDatas() == null || route.getQueueDatas().isEmpty());
    }

    private Map<String, Map<Long, String>> buildBrokerMap(TopicRouteData route) {
        Map<String, Map<Long, String>> result = new HashMap<>();
        if (route == null || route.getBrokerDatas() == null) {
            return result;
        }
        for (BrokerData brokerData : route.getBrokerDatas()) {
            Map<Long, String> addrs = brokerData.getBrokerAddrs();
            if (addrs != null) {
                result.put(brokerData.getBrokerName(), new HashMap<>(addrs));
            } else {
                result.put(brokerData.getBrokerName(), new HashMap<>());
            }
        }
        return result;
    }

    private Map<String, QueueData> buildQueueMap(TopicRouteData route) {
        Map<String, QueueData> result = new HashMap<>();
        if (route == null || route.getQueueDatas() == null) {
            return result;
        }
        for (QueueData queueData : route.getQueueDatas()) {
            result.put(queueData.getBrokerName(), queueData);
        }
        return result;
    }

    /**
     * Build a TopicRouteSnapshot from TopicRouteData.
     */
    public TopicRouteSnapshot buildSnapshot(String topic, TopicRouteData route) {
        TopicRouteSnapshot snapshot = new TopicRouteSnapshot();
        snapshot.setTopic(topic);

        if (route == null) {
            snapshot.setBrokers(new ArrayList<>());
            snapshot.setQueues(new ArrayList<>());
            return snapshot;
        }

        // Build broker info list
        List<TopicRouteSnapshot.BrokerInfo> brokers = new ArrayList<>();
        if (route.getBrokerDatas() != null) {
            for (BrokerData bd : route.getBrokerDatas()) {
                TopicRouteSnapshot.BrokerInfo info = new TopicRouteSnapshot.BrokerInfo();
                info.setCluster(bd.getCluster());
                info.setBrokerName(bd.getBrokerName());
                if (bd.getBrokerAddrs() != null) {
                    info.setBrokerAddrs(new HashMap<>(bd.getBrokerAddrs()));
                }
                brokers.add(info);
            }
        }
        snapshot.setBrokers(brokers);

        // Build queue info list
        List<TopicRouteSnapshot.QueueInfo> queues = new ArrayList<>();
        if (route.getQueueDatas() != null) {
            for (QueueData qd : route.getQueueDatas()) {
                TopicRouteSnapshot.QueueInfo info = new TopicRouteSnapshot.QueueInfo();
                info.setBrokerName(qd.getBrokerName());
                info.setReadQueueNums(qd.getReadQueueNums());
                info.setWriteQueueNums(qd.getWriteQueueNums());
                info.setPerm(qd.getPerm());
                queues.add(info);
            }
        }
        snapshot.setQueues(queues);

        return snapshot;
    }
}