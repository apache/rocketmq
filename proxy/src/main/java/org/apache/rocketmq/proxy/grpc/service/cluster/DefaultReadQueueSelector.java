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

import apache.rocketmq.v2.ReceiveMessageRequest;
import io.grpc.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;

public class DefaultReadQueueSelector implements ReadQueueSelector {

    private final TopicRouteCache topicRouteCache;

    public DefaultReadQueueSelector(TopicRouteCache topicRouteCache) {
        this.topicRouteCache = topicRouteCache;
    }

    @Override
    public SelectableMessageQueue select(Context ctx, ReceiveMessageRequest request, PopMessageRequestHeader requestHeader) {
        SelectableMessageQueue messageQueue = null;
        try {
            String topic = requestHeader.getTopic();

            if (request.hasMessageQueue() && request.getMessageQueue().hasBroker()) {
                String brokerName = request.getMessageQueue().getBroker().getName();
                if (StringUtils.isNotBlank(brokerName)) {
                    messageQueue = topicRouteCache.selectReadBrokerByName(topic, brokerName);
                }
            }

            if (messageQueue == null) {
                messageQueue = topicRouteCache.selectOneReadBroker(topic, null);
            }
            return messageQueue;
        } catch (Throwable t) {
            return null;
        }
    }
}
