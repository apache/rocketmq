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

import java.util.List;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class ClusterTopicRouteService extends TopicRouteService {
    private final RouteEventSubscriber eventSubscriber;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public ClusterTopicRouteService(MQClientAPIFactory mqClientAPIFactory) {
        super(mqClientAPIFactory);

        this.eventSubscriber = new RouteEventSubscriber(topic -> {
            markCacheDirty(topic);
        });
    }
    @Override
    public void start() throws Exception {
        super.start();

        try {
            eventSubscriber.start();
            log.info("Route event subscriber started after parent initialization");
        } catch (Exception e) {
            log.error("Failed to start route event subscriber", e);
        }
    }

    @Override
    public void shutdown() throws Exception {
        try {
            eventSubscriber.shutdown();
        } catch (Exception e) {
            log.error("Error shutting down event subscriber", e);
        }

        super.shutdown();
    }
    @Override
    public MessageQueueView getCurrentMessageQueueView(ProxyContext ctx, String topicName) throws Exception {
        return getAllMessageQueueView(ctx, topicName);
    }

    @Override
    public ProxyTopicRouteData getTopicRouteForProxy(ProxyContext ctx, List<Address> requestHostAndPortList,
        String topicName) throws Exception {
        TopicRouteData topicRouteData = getAllMessageQueueView(ctx, topicName).getTopicRouteData();
        return new ProxyTopicRouteData(topicRouteData, requestHostAndPortList);
    }

    @Override
    public String getBrokerAddr(ProxyContext ctx, String brokerName) throws Exception {
        TopicRouteWrapper topicRouteWrapper = getAllMessageQueueView(ctx, brokerName).getTopicRouteWrapper();
        return topicRouteWrapper.getMasterAddr(brokerName);
    }

    @Override
    public AddressableMessageQueue buildAddressableMessageQueue(ProxyContext ctx, MessageQueue messageQueue) throws Exception {
        String brokerAddress = getBrokerAddr(ctx, messageQueue.getBrokerName());
        return new AddressableMessageQueue(messageQueue, brokerAddress);
    }
}
