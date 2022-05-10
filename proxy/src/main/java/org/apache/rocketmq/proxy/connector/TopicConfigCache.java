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
package org.apache.rocketmq.proxy.connector;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.AbstractCacheLoader;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.connector.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;

public class TopicConfigCache {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final TopicRouteCache topicRouteCache;
    private final ThreadPoolExecutor cacheRefreshExecutor;
    private final LoadingCache<String /* topicName */, TopicConfigAndQueueMapping> topicConfigCache;

    private final DefaultForwardClient defaultClient;

    public TopicConfigCache(TopicRouteCache topicRouteCache, DefaultForwardClient client) {
        this.topicRouteCache = topicRouteCache;
        this.defaultClient = client;

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getTopicConfigThreadPoolNums(),
            config.getTopicConfigThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "TopicConfigCacheRefresh",
            config.getTopicConfigThreadPoolQueueCapacity()
        );
        this.topicConfigCache = CacheBuilder.newBuilder()
            .maximumSize(config.getTopicConfigCacheMaxNum())
            .refreshAfterWrite(config.getTopicConfigCacheExpiredInSeconds(), TimeUnit.SECONDS)
            .build(new TopicConfigCacheLoader());
    }

    public TopicConfigAndQueueMapping getTopicConfigAndQueueMapping(String topic) throws Exception {
        return topicConfigCache.get(topic);
    }

    protected class TopicConfigCacheLoader extends AbstractCacheLoader<String, TopicConfigAndQueueMapping> {

        public TopicConfigCacheLoader() {
            super(cacheRefreshExecutor);
        }

        @Override
        protected TopicConfigAndQueueMapping getDirectly(String topic) throws Exception {
            MessageQueueWrapper messageQueueWrapper = topicRouteCache.getMessageQueue(topic);
            Optional<BrokerData> brokerDataOptional = messageQueueWrapper.getTopicRouteData().getBrokerDatas().stream().findAny();
            if (!brokerDataOptional.isPresent()) {
                throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST,
                    "No topic route info in name server for the topic: " + topic);
            }

            String brokerAddress = brokerDataOptional.get().selectBrokerAddr();
            return defaultClient.getTopicConfig(brokerAddress, topic);
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load topic config failed. topic:{}", key, e);
        }
    }
}
