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
import java.util.Map.Entry;
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

    private final ConcurrentMap<String, Long> refreshingTopics = new ConcurrentHashMap<>();
    
    public RouteCacheRefresher(LoadingCache<String, MessageQueueView> topicCache,
                              ThreadPoolExecutor executor) {
        this.topicCache = topicCache;
        this.executor = executor;

        this.scheduler = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("RouteCacheScheduler_")
        );
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::checkRefreshStatus, 1, 1, TimeUnit.SECONDS);

        scheduler.scheduleWithFixedDelay(this::processDirtyTopics, 50, 200, TimeUnit.MILLISECONDS);
    }

    public void markCacheDirty(String topic) {
        dirtyTopics.put(topic, System.currentTimeMillis());
        pendingTopics.offer(topic);
    }

    private void processDirtyTopics() {
        List<String> batch = new ArrayList<>();
        while (!pendingTopics.isEmpty() && batch.size() < 100) {
            batch.add(pendingTopics.poll());
        }

        for (String topic : batch) {
            executor.execute(() -> refreshSingleRoute(topic));
        }
    }
    
    private void refreshSingleRoute(String topic) {
        try {
            log.info("Refreshing route for: {}", topic);
            refreshingTopics.put(topic, System.currentTimeMillis());
            topicCache.refresh(topic);

        } catch (Exception e) {
            log.error("Refresh failed for: {}", topic, e);
            pendingTopics.offer(topic);
        }
    }

    private void checkRefreshStatus() {
        long currentTime = System.currentTimeMillis();
        List<String> completed = new ArrayList<>();

        for (Entry<String, Long> entry : refreshingTopics.entrySet()) {
            String topic = entry.getKey();
            long startTime = entry.getValue();

            if (currentTime - startTime > 5000) {
                log.warn("Refresh timeout for topic: {}", topic);
                completed.add(topic);
                pendingTopics.offer(topic);
            }

            if (topicCache.getIfPresent(topic) != null) {
                completed.add(topic);
                dirtyTopics.remove(topic);
                log.info("Refresh confirmed for topic: {}", topic);
            }
        }

        completed.forEach(refreshingTopics::remove);
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
