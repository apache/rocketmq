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
package org.apache.rocketmq.proxy.service.route;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.broker.route.RouteEventConstants;
import org.apache.rocketmq.broker.route.RouteEventType;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.alibaba.fastjson2.JSON;

public class RouteChangeNotifier {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    
    private final LoadingCache<String, MessageQueueView> topicCache;
    private final ThreadPoolExecutor routeRefreshExecutor;
    
    private final ConcurrentMap<String, Long> dirtyTopics = new ConcurrentHashMap<>();
    private final Queue<String> pendingTopics = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler;
    
    private final DefaultMQPushConsumer consumer;
    private final BiConsumer<String, Long> dirtyMarker;
    
    public RouteChangeNotifier(LoadingCache<String, MessageQueueView> topicCache) {
        this.topicCache = topicCache;

        this.scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("RouteUpdaterScheduler_")
        );

        this.routeRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            2,
            4,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "TopicRouteCacheRefresh",
            1000
        );

        this.consumer = new DefaultMQPushConsumer("PROXY_ROUTE_EVENT_GROUP");
        this.consumer.setMessageModel(MessageModel.BROADCASTING);
        this.consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        this.dirtyMarker = (topic, timeStamp) -> {
            markCacheDirty(topic, timeStamp);
        };
    }

    public void start() {
        startEventSubscription();

        scheduler.scheduleWithFixedDelay(this::processDirtyTopics, 50, 200, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }

        if (scheduler != null) {
            scheduler.shutdown();
        }

        if (routeRefreshExecutor != null) {
            routeRefreshExecutor.shutdown();
        }
    }
    
    private void startEventSubscription() {
        try {
            consumer.subscribe(TopicValidator.RMQ_ROUTE_EVENT_TOPIC, "*");
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                processEventMessages(msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
        } catch (MQClientException e) {
            log.error("Failed to start route event consumer", e);
        }
    }

    private void processEventMessages(List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            try {
                String json = new String(msg.getBody(), StandardCharsets.UTF_8);
                Map<String, Object> event = JSON.parseObject(json, Map.class);

                log.info("[ROUTE_EVENT]: accept event {}", event);
                RouteEventType eventType = RouteEventType.valueOf((String) event.get(RouteEventConstants.EVENT_TYPE));
                Long eventTimeStamp = (Long) event.get(RouteEventConstants.TIMESTAMP);

                switch (eventType) {
                    case START:
                    case SHUTDOWN:
                    case TOPIC_CHANGE:
                        List<String> affectedTopics = (List<String>) event.get(RouteEventConstants.AFFECTED_TOPICS);

                        if (affectedTopics != null) {
                            for (String topic : affectedTopics) {
                                dirtyMarker.accept(topic, eventTimeStamp);
                            }
                        } else {
                            log.info("[ROUTE_UPDATE] No affected topic specified in event: {}", event);
                        }
                        break;

                    default:
                        break;
                }
                
            } catch (Exception e) {
                log.error("[ROUTE_UPDATE]: Error processing route event", e);
            }
        }
    }

    public void markCacheDirty(String topic, long timeStamp) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - timeStamp > TimeUnit.MINUTES.toMillis(1)) {
            return;
        }

        dirtyTopics.put(topic, currentTime);
        pendingTopics.offer(topic);
    }

    public void markCompleted(String topic) {
        dirtyTopics.remove(topic);
    }

    private void processDirtyTopics() {
        List<String> batch = new ArrayList<>();
        while (!pendingTopics.isEmpty() && batch.size() < 100) {
            String topic = pendingTopics.poll();
            if (topic == null) break;

            Long timestamp = dirtyTopics.get(topic);
            if (timestamp == null) continue;

            if (System.currentTimeMillis() - timestamp > TimeUnit.MINUTES.toMillis(1)) {
                markCompleted(topic);
                log.error("refreshing topic: {} stop for delay", topic);
                continue;
            }

            batch.add(topic);
        }

        for (String topic : batch) {
            routeRefreshExecutor.execute(() -> {
                refreshSingleRoute(topic);
                markCompleted(topic);
            });
        }
    }

    private void refreshSingleRoute(String topic) {
        try {
            if (topicCache.getIfPresent(topic) == null) {
                return;
            }

            topicCache.refresh(topic);

        } catch (Exception e) {
            log.error("Refresh failed for: {}", topic, e);
        }
    }
}
