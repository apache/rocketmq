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
package org.apache.rocketmq.broker.loadbalance;

import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class AssignmentManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private transient BrokerController brokerController;

    private final ConcurrentHashMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<String, Set<MessageQueue>>();

    private ScheduledExecutorService scheduledExecutorService;

    private static final List<String> IGNORE_ROUTE_TOPICS = Lists.newArrayList(
        MixAll.CID_RMQ_SYS_PREFIX,
        MixAll.DEFAULT_CONSUMER_GROUP,
        MixAll.TOOLS_CONSUMER_GROUP,
        MixAll.FILTERSRV_CONSUMER_GROUP,
        MixAll.MONITOR_CONSUMER_GROUP,
        MixAll.ONS_HTTP_PROXY_GROUP,
        MixAll.CID_ONSAPI_PERMISSION_GROUP,
        MixAll.CID_ONSAPI_OWNER_GROUP,
        MixAll.CID_ONSAPI_PULL_GROUP
    );

    private final List<String> ignoreRouteTopics = Lists.newArrayList(IGNORE_ROUTE_TOPICS);

    public AssignmentManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        ignoreRouteTopics.add(brokerController.getBrokerConfig().getBrokerClusterName());
        ignoreRouteTopics.add(brokerController.getBrokerConfig().getBrokerName());
        scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("LoadBalanceManagerScheduledThread", brokerController.getBrokerIdentity()));
    }

    public void start() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask: failed to pull TopicRouteData from NameServer", e);
                }
            }
        }, 1000, this.brokerController.getBrokerConfig().getLoadBalancePollNameServerInterval(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<>(brokerController.getTopicConfigManager().getTopicConfigTable().keySet());

        LOOP:
        for (String topic : topicList) {
            for (String keyword : ignoreRouteTopics) {
                if (topic.contains(keyword) || TopicValidator.isSystemTopic(topic)) {
                    continue LOOP;
                }
            }

            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        try {
            TopicRouteData topicRouteData = this.brokerController.getBrokerOuterAPI().getTopicRouteInfoFromNameServer(topic, 1000 * 3);
            if (topicRouteData != null) {
                topicRouteData.setTopicQueueMappingByBroker(null);
                Set<MessageQueue> newSubscribeInfo = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                Set<MessageQueue> oldSubscribeInfo = topicSubscribeInfoTable.get(topic);
                boolean changed = !newSubscribeInfo.equals(oldSubscribeInfo);

                if (changed) {
                    log.info("the topic[{}] subscribe message queue changed, old[{}] ,new[{}]", topic, oldSubscribeInfo, newSubscribeInfo);
                    topicSubscribeInfoTable.put(topic, newSubscribeInfo);
                    return true;
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
            }
        } catch (Exception e) {
            if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                if (e instanceof MQBrokerException && ResponseCode.TOPIC_NOT_EXIST == ((MQBrokerException) e).getResponseCode()) {
                    // clean no used topic
                    cleanNoneRouteTopic(topic);
                }
            }
        }
        return false;
    }

    private void cleanNoneRouteTopic(String topic) {
        // clean no used topic
        topicSubscribeInfoTable.remove(topic);
    }

    public Set<MessageQueue> getTopicSubscribeInfo(String topic) {
        return topicSubscribeInfoTable.get(topic);
    }
}
