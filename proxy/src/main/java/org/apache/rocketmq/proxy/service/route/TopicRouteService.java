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

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.latency.Resolver;
import org.apache.rocketmq.client.latency.ServiceDetector;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class TopicRouteService extends AbstractStartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final MQClientAPIFactory mqClientAPIFactory;
    private MQFaultStrategy mqFaultStrategy;

    protected final LoadingCache<String /* topicName */, MessageQueueView> topicCache;
    protected final ScheduledExecutorService scheduledExecutorService;
    protected final ThreadPoolExecutor cacheRefreshExecutor;

    protected final ConcurrentMap<String, Long> dirtyTopics = new ConcurrentHashMap<>();
    protected final BlockingQueue<String> pendingTopics = new LinkedBlockingQueue<>();
    protected final ScheduledExecutorService lazyUpdateExecutor;
    protected final Object batchUpdateLock = new Object();

    public TopicRouteService(MQClientAPIFactory mqClientAPIFactory) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        this.scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("TopicRouteService_")
        );
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getTopicRouteServiceThreadPoolNums(),
            config.getTopicRouteServiceThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "TopicRouteCacheRefresh",
            config.getTopicRouteServiceThreadPoolQueueCapacity()
        );
        this.mqClientAPIFactory = mqClientAPIFactory;

        this.topicCache = Caffeine.newBuilder().maximumSize(config.getTopicRouteServiceCacheMaxNum())
            .expireAfterAccess(config.getTopicRouteServiceCacheExpiredSeconds(), TimeUnit.SECONDS)
            .refreshAfterWrite(config.getTopicRouteServiceCacheRefreshSeconds(), TimeUnit.SECONDS)
            .executor(cacheRefreshExecutor)
            .build(new CacheLoader<String, MessageQueueView>() {
                @Override
                public @Nullable MessageQueueView load(String topic) throws Exception {
                    try {
                        TopicRouteData topicRouteData = mqClientAPIFactory.getClient().getTopicRouteInfoFromNameServer(topic, Duration.ofSeconds(3).toMillis());
                        return buildMessageQueueView(topic, topicRouteData);
                    } catch (Exception e) {
                        if (TopicRouteHelper.isTopicNotExistError(e)) {
                            return MessageQueueView.WRAPPED_EMPTY_QUEUE;
                        }
                        throw e;
                    }
                }

                @Override
                public @Nullable MessageQueueView reload(@NonNull String key,
                    @NonNull MessageQueueView oldValue) throws Exception {
                    try {
                        return load(key);
                    } catch (Exception e) {
                        log.warn(String.format("reload topic route from namesrv. topic: %s", key), e);
                        return oldValue;
                    }
                }
            });
        ServiceDetector serviceDetector = new ServiceDetector() {
            @Override
            public boolean detect(String endpoint, long timeoutMillis) {
                Optional<String> candidateTopic = pickTopic();
                if (!candidateTopic.isPresent()) {
                    return false;
                }
                try {
                    GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
                    requestHeader.setTopic(candidateTopic.get());
                    requestHeader.setQueueId(0);
                    Long maxOffset = mqClientAPIFactory.getClient().getMaxOffset(endpoint, requestHeader, timeoutMillis).get();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        };
        mqFaultStrategy = new MQFaultStrategy(extractClientConfigFromProxyConfig(config), new Resolver() {
            @Override
            public String resolve(String name) {
                try {
                    String brokerAddr = getBrokerAddr(ProxyContext.createForInner("MQFaultStrategy"), name);
                    return brokerAddr;
                } catch (Exception e) {
                    return null;
                }
            }
        }, serviceDetector);
        this.lazyUpdateExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("RouteLazyUpdate_"));

        lazyUpdateExecutor.scheduleWithFixedDelay(() -> {
            batchRefreshRoutes();
        }, 50, 200, TimeUnit.MILLISECONDS);

        this.init();
    }

    // pickup one topic in the topic cache
    private Optional<String> pickTopic() {
        if (topicCache.asMap().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(topicCache.asMap().keySet().iterator().next());
    }

    protected void init() {
        this.appendShutdown(this.scheduledExecutorService::shutdown);
        this.appendStartAndShutdown(this.mqClientAPIFactory);
    }

    @Override
    public void shutdown() throws Exception {
        if (this.mqFaultStrategy.isStartDetectorEnable()) {
            mqFaultStrategy.shutdown();
        }
    }

    @Override
    public void start() throws Exception {
        if (this.mqFaultStrategy.isStartDetectorEnable()) {
            this.mqFaultStrategy.startDetector();
        }
    }

    public ClientConfig extractClientConfigFromProxyConfig(ProxyConfig proxyConfig) {
        ClientConfig tempClientConfig = new ClientConfig();
        tempClientConfig.setSendLatencyEnable(proxyConfig.getSendLatencyEnable());
        tempClientConfig.setStartDetectorEnable(proxyConfig.getStartDetectorEnable());
        tempClientConfig.setDetectTimeout(proxyConfig.getDetectTimeout());
        tempClientConfig.setDetectInterval(proxyConfig.getDetectInterval());
        return tempClientConfig;
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation,
                                boolean reachable) {
        checkSendFaultToleranceEnable();
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation, reachable);
    }

    public void checkSendFaultToleranceEnable() {
        boolean hotLatencySwitch = ConfigurationManager.getProxyConfig().isSendLatencyEnable();
        boolean hotDetectorSwitch = ConfigurationManager.getProxyConfig().isStartDetectorEnable();
        this.mqFaultStrategy.setSendLatencyFaultEnable(hotLatencySwitch);
        this.mqFaultStrategy.setStartDetectorEnable(hotDetectorSwitch);
    }

    public MQFaultStrategy getMqFaultStrategy() {
        return this.mqFaultStrategy;
    }

    public MessageQueueView getAllMessageQueueView(ProxyContext ctx, String topicName) throws Exception {
        return getCacheMessageQueueWrapper(this.topicCache, topicName);
    }

    public abstract MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) throws Exception;

    public abstract ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception;

    public abstract String getBrokerAddr(ProxyContext ctx, String brokerName) throws Exception;

    public abstract AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx, MessageQueue messageQueue) throws Exception;

    protected static MessageQueueView getCacheMessageQueueWrapper(LoadingCache<String, MessageQueueView> topicCache,
        String key) throws Exception {
        MessageQueueView res = topicCache.get(key);
        if (res != null && res.isEmptyCachedQueue()) {
            throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST,
                "No topic route info in name server for the topic: " + key);
        }
        return res;
    }

    protected static boolean isTopicRouteValid(TopicRouteData routeData) {
        return routeData != null && routeData.getQueueDatas() != null && !routeData.getQueueDatas().isEmpty()
            && routeData.getBrokerDatas() != null && !routeData.getBrokerDatas().isEmpty();
    }

    protected MessageQueueView buildMessageQueueView(String topic, TopicRouteData topicRouteData) {
        if (isTopicRouteValid(topicRouteData)) {
            MessageQueueView tmp = new MessageQueueView(topic, topicRouteData, TopicRouteService.this.getMqFaultStrategy());
            log.debug("load topic route from namesrv. topic: {}, queue: {}", topic, tmp);
            return tmp;
        }
        return MessageQueueView.WRAPPED_EMPTY_QUEUE;
    }

    protected void markCacheDirty(String topic) {
        if (dirtyTopics.putIfAbsent(topic, System.currentTimeMillis()) == null) {
            pendingTopics.offer(topic);
        }
    }

    private void batchRefreshRoutes() {
        if (pendingTopics.isEmpty()) return;

        List<String> refreshList = new ArrayList<>(100);
        synchronized (batchUpdateLock) {
            int count = 0;
            while (!pendingTopics.isEmpty() && count < 100) {
                String topic = pendingTopics.poll();
                if (topic != null) {
                    refreshList.add(topic);
                    count++;
                }
            }
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String topic : refreshList) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    refreshSingleRoute(topic);
                } catch (Exception e) {
                    log.warn("Refresh topic route failed: {}", topic, e);
                    pendingTopics.offer(topic);
                }
            }, cacheRefreshExecutor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void refreshSingleRoute(String topic) {
        try {
            TopicRouteData routeData = mqClientAPIFactory.getClient()
                .getTopicRouteInfoFromNameServer(topic, 1000);

            MessageQueueView newView = buildMessageQueueView(topic, routeData);
            topicCache.put(topic, newView);

            dirtyTopics.remove(topic);
        } catch (Exception e) {

        }
    }

}
