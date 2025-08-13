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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.rocketmq.broker.route.RouteEventConstants;
import org.apache.rocketmq.broker.route.RouteEventType;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import com.alibaba.fastjson2.JSON;

public class RouteEventSubscriber {
    private final Consumer<String> dirtyMarker;
    private final TopicRouteService topicRouteService;
    private final DefaultMQPushConsumer consumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public RouteEventSubscriber(TopicRouteService topicRouteService, Consumer<String> dirtyMarker) {
        this.topicRouteService = topicRouteService;
        this.dirtyMarker = dirtyMarker;
        this.consumer = new DefaultMQPushConsumer("PROXY_ROUTE_EVENT_GROUP");
        this.consumer.setMessageModel(MessageModel.BROADCASTING);
    }
    public void start() {
        try {
            consumer.subscribe(TopicValidator.RMQ_ROUTE_EVENT_TOPIC, "*");
            LOGGER.warn("Subscribed to system topic: {}", TopicValidator.RMQ_ROUTE_EVENT_TOPIC);

            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                LOGGER.warn("[ROUTE_UPDATE] Received {} events", msgs.size());
                processMessages(msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
            LOGGER.warn("Route event consumer started");
        } catch (MQClientException e) {
            LOGGER.error("Failed to start route event consumer", e);
        }
    }

    private void processMessages(List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            try {
                String json = new String(msg.getBody(), StandardCharsets.UTF_8);
                Map<String, Object> event = JSON.parseObject(json, Map.class);
                LOGGER.info("[ROUTE_UPDATE]: Received route event: {} consumer {}", event, this.consumer.getInstanceName());

                String brokerName = (String) event.get(RouteEventConstants.BROKER_NAME);
                RouteEventType eventType = RouteEventType.valueOf((String) event.get(RouteEventConstants.EVENT_TYPE));

                Set<String> topics  = this.topicRouteService.getBrokerTopics(brokerName);

                if (topics == null || topics.isEmpty()) {
                    LOGGER.warn("[ROUTE_UPDATE] No affected topics in event");
                    continue;
                }

                if (eventType == RouteEventType.SHUTDOWN) {
                    topicRouteService.removeBrokerTopics(brokerName);
                }

                for (String topic : topics) {
                    LOGGER.warn("[ROUTE_UPDATE] Processing topic: {}", topic);
                    dirtyMarker.accept(topic);
                }
            } catch (Exception e) {
                LOGGER.error("[ROUTE_UPDATE]: Error processing route event", e);
            }
        }
    }

    public void shutdown() {
        consumer.shutdown();
    }
}
