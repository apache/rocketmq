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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.ThreadFactoryImpl;

import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.rocketmq.common.utils.ThreadUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RouteCacheRefresher {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final LoadingCache<String, MessageQueueView> topicCache;
    private final ThreadPoolExecutor executor;

    private final ConcurrentMap<String, Long> dirtyTopics = new ConcurrentHashMap<>();
    private final Queue<String> pendingTopics = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler;

    public RouteCacheRefresher(LoadingCache<String, MessageQueueView> topicCache,
                              ThreadPoolExecutor executor) {
        this.topicCache = topicCache;
        this.executor = executor;

        this.scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("RouteCacheScheduler_")
        );
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::processDirtyTopics, 50, 200, TimeUnit.MILLISECONDS);
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
            executor.execute(() -> {
                refreshSingleRoute(topic);
                markCompleted(topic);
            });
        }
    }

    private void refreshSingleRoute(String topic) {
        try {
            if (topicCache.getIfPresent(topic) == null) {
                log.warn("No cache entry found for topic: {}", topic);
                return;
            }

            topicCache.refresh(topic);
            log.info("[ROUTE_NOTIFICATION]: Refresh topic: {}", topic);

        } catch (Exception e) {
            log.error("Refresh failed for: {}", topic, e);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
