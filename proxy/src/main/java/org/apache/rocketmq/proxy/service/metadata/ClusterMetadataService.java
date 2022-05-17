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

package org.apache.rocketmq.proxy.service.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;

public class ClusterMetadataService extends AbstractMetadataService {
    private final LoadingCache<String, TopicConfigAndQueueMapping> topicCache;
    private TopicRouteService topicRouteService;
    private final static TopicConfigAndQueueMapping EMPTY_TOPIC_CONFIG = new TopicConfigAndQueueMapping();

    public ClusterMetadataService(TopicRouteService topicRouteService, MQClientAPIFactory mqClientAPIFactory) {
        this.topicRouteService = topicRouteService;
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.topicCache = CacheBuilder.newBuilder()
            .maximumSize(config.getTopicConfigCacheMaxNum())
            .refreshAfterWrite(config.getTopicConfigCacheExpiredInSeconds(), TimeUnit.SECONDS)
            .build(new ClusterTopicConfigCacheLoader(mqClientAPIFactory));
    }

    @Override public TopicMessageType getTopicMessageType(String topic) {
        TopicConfigAndQueueMapping topicConfigAndQueueMapping;
        try {
            topicConfigAndQueueMapping = topicCache.get(topic);
        } catch (Exception e) {
            return TopicMessageType.UNSPECIFIED;
        }
        if (topicConfigAndQueueMapping.equals(EMPTY_TOPIC_CONFIG)) {
            return TopicMessageType.UNSPECIFIED;
        }
        return topicConfigAndQueueMapping.getTopicMessageType();
    }

    protected class ClusterTopicConfigCacheLoader extends AbstractTopicConfigCacheLoader {
        private final MQClientAPIFactory mqClientAPIFactory;

        public ClusterTopicConfigCacheLoader(MQClientAPIFactory mqClientAPIFactory) {
            this.mqClientAPIFactory = mqClientAPIFactory;
        }

        @Override protected TopicConfigAndQueueMapping loadTopicConfig(String topic) throws Exception {
            try {
                Optional<BrokerData> brokerDataOptional = topicRouteService.getAllMessageQueueView(topic).getTopicRouteData().getBrokerDatas().stream().findAny();
                if (brokerDataOptional.isPresent()) {
                    String brokerAddress = topicRouteService.getBrokerAddr(brokerDataOptional.get().getBrokerName());
                    return this.mqClientAPIFactory.getClient().getTopicConfig(brokerAddress, topic, 1000L);
                }
                return EMPTY_TOPIC_CONFIG;
            } catch (MQClientException e) {
                if (TopicRouteHelper.isTopicNotExistError(e)) {
                    log.warn("topic is not exist", e);
                    return EMPTY_TOPIC_CONFIG;
                }
                throw e;
            }
        }
    }
}
