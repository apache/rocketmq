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
package org.apache.rocketmq.proxy.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.Hashing;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.client.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.client.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.common.RetainCacheLoader;
import org.apache.rocketmq.proxy.common.RocketMQHelper;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;
import org.apache.rocketmq.proxy.configuration.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicRouteCache {

    private static final Logger log = LoggerFactory.getLogger(TopicRouteCache.class);

    private final LoadingCache<String /* topicName */, MessageQueueWrapper> topicCache;
    private final ThreadPoolExecutor cacheRefreshExecutor;

    private final DefaultForwardClient defaultClient;

    public TopicRouteCache(DefaultForwardClient defaultClient) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        this.defaultClient = defaultClient;
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getTopicRouteThreadPoolNums(),
            config.getTopicRouteThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "TopicRouteCacheRefresh",
            config.getTopicRouteThreadPoolQueueCapacity()
        );
        this.topicCache = CacheBuilder.newBuilder()
            .maximumSize(config.getTopicRouteCacheMaxNum())
            .refreshAfterWrite(config.getTopicRouteCacheExpiredInSeconds(), TimeUnit.SECONDS)
            .build(new TopicRouteCacheLoader());
    }

    public MessageQueueWrapper getMessageQueue(String topicName) throws Exception {
        return getCacheMessageQueueWrapper(this.topicCache, topicName);
    }

    public SelectableMessageQueue selectOneWriteQueue(String topic, SelectableMessageQueue last) throws Exception {
        if (last == null) {
            return getMessageQueue(topic).getWriteSelector().selectOne(false);
        }
        return getMessageQueue(topic).getWriteSelector().selectNextQueue(last);
    }

    public SelectableMessageQueue selectOneWriteQueue(String topic, String brokerName, int queueId) throws Exception {
        return getMessageQueue(topic).getWriteSelector().selectOne(brokerName, queueId);
    }

    public SelectableMessageQueue selectOneWriteQueueByKey(String topic, String shardingKey, SelectableMessageQueue last) throws Exception {
        List<SelectableMessageQueue> writeQueues = getMessageQueue(topic).getWriteSelector().getQueues();
        int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
        return writeQueues.get(bucket);
    }

    protected static MessageQueueWrapper getCacheMessageQueueWrapper(LoadingCache<String, MessageQueueWrapper> topicCache, String key) throws Exception {
        MessageQueueWrapper res = topicCache.get(key);
        if (res.isEmptyCachedQueue()) {
            throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST,
                "No topic route info in name server for the topic: " + key);
        }
        return res;
    }

    protected static boolean isTopicRouteValid(TopicRouteData routeData) {
        return routeData != null && routeData.getQueueDatas() != null && !routeData.getQueueDatas().isEmpty()
            && routeData.getBrokerDatas() != null && !routeData.getBrokerDatas().isEmpty();
    }

    protected abstract class AbstractTopicRouteCacheLoader extends RetainCacheLoader<String, MessageQueueWrapper> {

        public AbstractTopicRouteCacheLoader() {
            super(cacheRefreshExecutor);
        }

        protected abstract String loaderName();

        protected abstract TopicRouteData loadTopicRouteData(String topic) throws Exception;

        protected boolean refreshBrokerAddrTable() {
            return false;
        }

        @Override
        public MessageQueueWrapper getDirectly(String topic) throws Exception {
            try {
                TopicRouteData topicRouteData = loadTopicRouteData(topic);

                if (isTopicRouteValid(topicRouteData)) {
                    MessageQueueWrapper tmp = new MessageQueueWrapper(topic, topicRouteData);
                    log.info("load {} from namesrv. topic: {}, queue: {}", loaderName(), topic, tmp);
                    return tmp;
                }
                return MessageQueueWrapper.WRAPPED_EMPTY_QUEUE;
            } catch (Exception e) {
                if (RocketMQHelper.isTopicNotExistError(e)) {
                    return MessageQueueWrapper.WRAPPED_EMPTY_QUEUE;
                }
                throw e;
            }
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load {} from namesrv failed. topic:{}", loaderName(), key, e);
        }
    }

    protected class TopicRouteCacheLoader extends AbstractTopicRouteCacheLoader {

        @Override
        protected String loaderName() {
            return "topicRoute";
        }

        @Override
        protected TopicRouteData loadTopicRouteData(String topic) throws Exception {
            return defaultClient.getTopicRouteInfoFromNameServer(topic, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
        }
    }
}
