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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.SendMessageRequest;
import io.grpc.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultWriteQueueSelector implements WriteQueueSelector {

    private static final Logger log = LoggerFactory.getLogger(DefaultWriteQueueSelector.class);
    protected final TopicRouteCache topicRouteCache;

    public DefaultWriteQueueSelector(TopicRouteCache topicRouteCache) {
        this.topicRouteCache = topicRouteCache;
    }

    @Override
    public SelectableMessageQueue selectQueue(Context ctx, SendMessageRequest request,
        SendMessageRequestHeader requestHeader,
        org.apache.rocketmq.common.message.Message message) {
        try {
            String topic = requestHeader.getTopic();
            String brokerName = "";
            if (request.hasPartition()) {
                brokerName = request.getPartition().getBroker().getName();
            }
            Integer queueId = requestHeader.getQueueId();
            String shardingKey = message.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
            SelectableMessageQueue addressableMessageQueue;
            if (!StringUtils.isBlank(brokerName) && queueId != null) {
                // Grpc client sendSelect situation
                addressableMessageQueue = selectTargetQueue(topic, brokerName, queueId);
            } else if (shardingKey != null) {
                // With shardingKey
                addressableMessageQueue = selectOrderQueue(topic, shardingKey);
            } else {
                addressableMessageQueue = selectNormalQueue(topic);
            }
            return addressableMessageQueue;
        } catch (Exception e) {
            log.error("error when select queue in DefaultMessageQueueSelector. request: {}", request, e);
            return null;
        }
    }

    protected SelectableMessageQueue selectNormalQueue(String topic) throws Exception {
        return this.topicRouteCache.selectOneWriteQueue(topic, null);
    }

    protected SelectableMessageQueue selectTargetQueue(String topic, String brokerName,
        int queueId) throws Exception {
        return this.topicRouteCache.selectOneWriteQueue(topic, brokerName, queueId);
    }

    protected SelectableMessageQueue selectOrderQueue(String topic, String shardingKey) throws Exception {
        return this.topicRouteCache.selectOneWriteQueueByKey(topic, shardingKey);
    }
}
