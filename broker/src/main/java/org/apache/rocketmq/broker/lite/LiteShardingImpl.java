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

package org.apache.rocketmq.broker.lite;

import com.google.common.hash.Hashing;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * Default {@link LiteSharding} implementation: assigns each LMQ to a broker
 * queue via consistent hashing of the lite topic segment. The sharding
 * result is consumed by {@code AbstractLiteLifecycleManager} to decide
 * whether a subscription should be accepted locally.
 *
 * <p>When the parent topic has no known route, or the LMQ name does not
 * parse into a lite topic, the current broker is returned as a fallback so
 * that subscription requests are not silently dropped during transient
 * routing gaps.
 */
public class LiteShardingImpl implements LiteSharding {

    private final BrokerController brokerController;
    private final TopicRouteInfoManager topicRouteInfoManager;

    public LiteShardingImpl(BrokerController brokerController, TopicRouteInfoManager topicRouteInfoManager) {
        this.brokerController = brokerController;
        this.topicRouteInfoManager = topicRouteInfoManager;
    }

    /**
     * Compute the broker that owns the given LMQ via consistent hashing of
     * the lite topic segment over the parent's write queues. Falls back to
     * the current broker name when the parent route is missing, has no
     * queues, or the LMQ name is not a valid lite topic.
     */
    @Override
    public String shardingByLmqName(String parentTopic, String lmqName) {
        TopicPublishInfo topicPublishInfo = topicRouteInfoManager.tryToFindTopicPublishInfo(parentTopic);
        if (topicPublishInfo == null) {
            // if topic not exist, return current broker
            return brokerController.getBrokerConfig().getBrokerName();
        }
        List<MessageQueue> writeQueues = topicPublishInfo.getMessageQueueList();
        if (CollectionUtils.isEmpty(writeQueues)) {
            return brokerController.getBrokerConfig().getBrokerName();
        }
        String liteTopic = LiteUtil.getLiteTopic(lmqName);
        if (StringUtils.isEmpty(liteTopic)) {
            return brokerController.getBrokerConfig().getBrokerName();
        }
        int bucket = Hashing.consistentHash(liteTopic.hashCode(), writeQueues.size());
        MessageQueue targetQueue = writeQueues.get(bucket);
        return targetQueue.getBrokerName();
    }
}
