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
package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.SendMessageRequest;
import io.grpc.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;

public class DefaultWriteQueueSelector implements WriteQueueSelector {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected final TopicRouteCache topicRouteCache;

    public DefaultWriteQueueSelector(TopicRouteCache topicRouteCache) {
        this.topicRouteCache = topicRouteCache;
    }

    @Override
    public SelectableMessageQueue selectQueue(
        Context ctx,
        SendMessageRequest request
    ) {
        try {
            if (request.getMessagesCount() <= 0) {
                throw new ProxyException(Code.MESSAGE_CORRUPTED, "no message to send");
            }
            Message message = request.getMessages(0);
            String topic = GrpcConverter.wrapResourceWithNamespace(message.getTopic());
            String shardingKey = null;
            if (request.getMessagesCount() == 1) {
                shardingKey = message.getSystemProperties().getMessageGroup();
            }
            SelectableMessageQueue targetMessageQueue;
            if (StringUtils.isNotEmpty(shardingKey)) {
                // With shardingKey
                targetMessageQueue = selectOrderQueue(topic, shardingKey);
            } else {
                targetMessageQueue = selectNormalQueue(topic);
            }
            return targetMessageQueue;
        } catch (Exception e) {
            log.error("error when select queue in DefaultMessageQueueSelector. request: {}", request, e);
            return null;
        }
    }

    protected SelectableMessageQueue selectNormalQueue(String topic) throws Exception {
        return this.topicRouteCache.selectOneWriteQueue(topic, null);
    }

    protected SelectableMessageQueue selectOrderQueue(String topic, String shardingKey) throws Exception {
        return this.topicRouteCache.selectOneWriteQueueByKey(topic, shardingKey);
    }
}
