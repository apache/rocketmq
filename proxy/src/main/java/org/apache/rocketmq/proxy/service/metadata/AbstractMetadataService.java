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

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.AbstractCacheLoader;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;

public abstract class AbstractMetadataService extends AbstractStartAndShutdown implements MetadataService {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final ThreadPoolExecutor cacheRefreshExecutor;

    public AbstractMetadataService() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getTopicConfigThreadPoolNums(),
            config.getTopicConfigThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            "MetadataCacheRefresh",
            config.getTopicConfigThreadPoolQueueCapacity()
        );
    }

    public abstract TopicMessageType getTopicMessageType(String topic);

    protected abstract class AbstractTopicConfigCacheLoader extends AbstractCacheLoader<String, TopicConfigAndQueueMapping> {

        public AbstractTopicConfigCacheLoader() {
            super(cacheRefreshExecutor);
        }

        protected abstract TopicConfigAndQueueMapping loadTopicConfig(String topic) throws Exception;

        @Override
        public TopicConfigAndQueueMapping getDirectly(String topic) throws Exception {
            return loadTopicConfig(topic);
        }

        @Override
        protected void onErr(String key, Exception e) {
            log.error("load topic config failed. topic:{}", key, e);
        }
    }
}
