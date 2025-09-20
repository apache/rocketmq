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
import java.util.function.BiConsumer;

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
    private final BiConsumer<String, Long> dirtyMarker;
    private final DefaultMQPushConsumer consumer;
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public RouteEventSubscriber(BiConsumer<String, Long> dirtyMarker) {
        this.dirtyMarker = dirtyMarker;
        this.consumer = new DefaultMQPushConsumer("PROXY_ROUTE_EVENT_GROUP");
        this.consumer.setMessageModel(MessageModel.BROADCASTING);
    }
    public void start() {
        try {
            consumer.subscribe(TopicValidator.RMQ_ROUTE_EVENT_TOPIC, "*");
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                processMessages(msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
        } catch (MQClientException e) {
            LOGGER.error("Failed to start route event consumer", e);
        }
    }

    private void processMessages(List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            try {
                String json = new String(msg.getBody(), StandardCharsets.UTF_8);
                Map<String, Object> event = JSON.parseObject(json, Map.class);

                LOGGER.info("[ROUTE_EVENT]: accept event {}", event);
                RouteEventType eventType = RouteEventType.valueOf((String) event.get(RouteEventConstants.EVENT_TYPE));
                Long eventTimeStamp = (Long) event.get(RouteEventConstants.TIMESTAMP);


                switch (eventType) {
                    case START:
                    case SHUTDOWN:
                    case TOPIC_CHANGE:
                        List<String> affectedTopics = (List<String>) event.get(RouteEventConstants.AFFECTED_TOPICS);

                        if (affectedTopics != null) {
                            for (String topic : affectedTopics) {
                                dirtyMarker.accept(topic, eventTimeStamp);
                            }
                        } else {
                            LOGGER.info("[ROUTE_UPDATE] No affected topic specified in event: {}", event);
                        }
                        break;

                    default:
                        break;
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
