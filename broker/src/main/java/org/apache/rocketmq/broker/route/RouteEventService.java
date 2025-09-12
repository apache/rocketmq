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
package org.apache.rocketmq.broker.route;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;

import com.alibaba.fastjson2.JSON;

public class RouteEventService {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public RouteEventService(BrokerController brokerController) {
        this.brokerController = brokerController;
        LOG.info("RouteEventService initialized for broker: {}",
            brokerController.getBrokerConfig().getBrokerName());
    }

    public void publishEvent(RouteEventType eventType) {
        publishEvent(eventType, null);
    }

    public void publishEvent(RouteEventType eventType, String affectedtopic) {
        if (!brokerController.getBrokerConfig().isEnableRouteChangeNotification()) {
            return;
        }
        Map<String, Object> eventData = new HashMap<>();
        PutMessageResult result;
        eventData.put(RouteEventConstants.EVENT_TYPE, eventType.name());
        eventData.put(RouteEventConstants.BROKER_NAME, brokerController.getBrokerConfig().getBrokerName());
        eventData.put(RouteEventConstants.BROKER_ID, brokerController.getBrokerConfig().getBrokerId());
        eventData.put(RouteEventConstants.TIMESTAMP, System.currentTimeMillis());
        if (affectedtopic != null) {
            eventData.put(RouteEventConstants.AFFECTED_TOPIC, affectedtopic);
        }

        MessageExtBrokerInner msg = createEventMessage(eventData);

        try {
            result = brokerController.getMessageStore().putMessage(msg);
        }
        catch (Exception e) {
            LOG.error("[ROUTE_EVENT] Failed to publish event: {}", eventType, e);
            return;
        }
        brokerController.getMessageStore().flush();

        if (!result.isOk()) {
            LOG.error("[ROUTE_EVENT] Publish failed: {}", result.getPutMessageStatus());
        }
    }

    private MessageExtBrokerInner createEventMessage(Map<String, Object> eventData) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TopicValidator.RMQ_ROUTE_EVENT_TOPIC);
        msg.setBody(JSON.toJSONString(eventData).getBytes(StandardCharsets.UTF_8));
        msg.setTags(eventData.get(RouteEventConstants.EVENT_TYPE).toString());
        msg.setQueueId(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(brokerController.getStoreHost());
        msg.setStoreHost(brokerController.getStoreHost());
        msg.setSysFlag(0);

        return msg;
    }
}
