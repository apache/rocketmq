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
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;

public class LocalMetadataService extends AbstractMetadataService {
    private final LoadingCache<String, TopicConfigAndQueueMapping> topicCache;

    public LocalMetadataService(BrokerController brokerController) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        this.topicCache = CacheBuilder.newBuilder()
            .maximumSize(config.getTopicConfigCacheMaxNum())
            .refreshAfterWrite(config.getTopicConfigCacheExpiredInSeconds(), TimeUnit.SECONDS)
            .build(new LocalTopicConfigCacheLoader(brokerController));
    }

    @Override
    public TopicMessageType getTopicMessageType(String topic) {
        try {
            TopicConfigAndQueueMapping topicConfigAndQueueMapping = topicCache.get(topic);
            if (topicConfigAndQueueMapping == null) {
                return TopicMessageType.UNSPECIFIED;
            }
            return topicConfigAndQueueMapping.getTopicMessageType();
        } catch (Exception e) {
            log.error("getTopicMessageType error", topic, e);
            return TopicMessageType.UNSPECIFIED;
        }
    }

    protected class LocalTopicConfigCacheLoader extends AbstractTopicConfigCacheLoader {
        private final BrokerController brokerController;

        public LocalTopicConfigCacheLoader(BrokerController brokerController) {
            this.brokerController = brokerController;
        }

        @Override protected TopicConfigAndQueueMapping loadTopicConfig(String topic) throws Exception {
            TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
            return new TopicConfigAndQueueMapping(topicConfig, null);
        }
    }
}
