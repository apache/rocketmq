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

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;

import com.alibaba.fastjson2.JSON;

public class RouteEventSubscriber {
    private final Consumer<String> dirtyMarker;
    private final DefaultMQPushConsumer consumer;

    public RouteEventSubscriber(Consumer<String> dirtyMarker) {
        this.dirtyMarker = dirtyMarker;
        this.consumer = createSimplifiedConsumer();
        startListening();
    }

    private DefaultMQPushConsumer createSimplifiedConsumer() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(
            "ProxyRouteSubGroup_" + ManagementFactory.getRuntimeMXBean().getName()
        );

        consumer.setNamesrvAddr(config.getNamesrvAddr());
        consumer.setPullBatchSize(10);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);

        return consumer;
    }

    private void startListening() {
        try {
            consumer.subscribe(TopicValidator.RMQ_ROUTE_EVENT_TOPIC, "*");

            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                processMessages(msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
        } catch (MQClientException e) {
        }
    }

    private void processMessages(List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            try {
                String json = new String(msg.getBody(), StandardCharsets.UTF_8);
                Map<String, Object> event = JSON.parseObject(json, Map.class);

                Object[] topics = (Object[]) event.get("affectedTopic");
                for (Object topicObj : topics) {
                    String topic = (String) topicObj;
                    dirtyMarker.accept(topic);
                }
            } catch (Exception e) {
            }
        }
    }

    public void shutdown() {
        consumer.shutdown();
    }
}