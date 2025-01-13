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
package org.apache.rocketmq.broker.topic;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

public class TopicRouteInfoManager {

    private static final long GET_TOPIC_ROUTE_TIMEOUT = 3000L;
    private static final long LOCK_TIMEOUT_MILLIS = 3000L;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final Lock lockNamesrv = new ReentrantLock();
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduledExecutorService;
    private BrokerController brokerController;

    public TopicRouteInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void start() {
        this.scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TopicRouteInfoManagerScheduledThread"));

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                updateTopicRouteInfoFromNameServer();
            } catch (Exception e) {
                log.error("ScheduledTask: failed to pull TopicRouteData from NameServer", e);
            }
        }, 1000, this.brokerController.getBrokerConfig().getLoadBalancePollNameServerInterval(), TimeUnit.MILLISECONDS);
    }

    private void updateTopicRouteInfoFromNameServer() {
        final Set<String> topicSetForPopAssignment = this.topicSubscribeInfoTable.keySet();
        final Set<String> topicSetForEscapeBridge = this.topicRouteTable.keySet();
        final Set<String> topicsAll = Sets.union(topicSetForPopAssignment, topicSetForEscapeBridge);

        for (String topic : topicsAll) {
            boolean isNeedUpdatePublishInfo = topicSetForEscapeBridge.contains(topic);
            boolean isNeedUpdateSubscribeInfo = topicSetForPopAssignment.contains(topic);
            updateTopicRouteInfoFromNameServer(topic, isNeedUpdatePublishInfo, isNeedUpdateSubscribeInfo);
        }
    }

    public void updateTopicRouteInfoFromNameServer(String topic, boolean isNeedUpdatePublishInfo,
        boolean isNeedUpdateSubscribeInfo) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    final TopicRouteData topicRouteData = this.brokerController.getBrokerOuterAPI()
                        .getTopicRouteInfoFromNameServer(topic, GET_TOPIC_ROUTE_TIMEOUT);
                    if (null == topicRouteData) {
                        log.warn("TopicRouteInfoManager: updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}.", topic);
                        return;
                    }

                    if (isNeedUpdateSubscribeInfo) {
                        this.updateSubscribeInfoTable(topicRouteData, topic);
                    }

                    if (isNeedUpdatePublishInfo) {
                        this.updateTopicRouteTable(topic, topicRouteData);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                } catch (MQBrokerException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    if (!NamespaceUtil.isRetryTopic(topic)
                        && ResponseCode.TOPIC_NOT_EXIST == e.getResponseCode()) {
                        // clean no used topic
                        cleanNoneRouteTopic(topic);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }
    }

    private boolean updateTopicRouteTable(String topic, TopicRouteData topicRouteData) {
        TopicRouteData old = this.topicRouteTable.get(topic);
        boolean changed = topicRouteData.topicRouteDataChanged(old);
        if (!changed) {
            if (!this.isNeedUpdateTopicRouteInfo(topic)) {
                return false;
            }
        } else {
            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
        }

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
        }

        TopicPublishInfo publishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
        publishInfo.setHaveTopicRouterInfo(true);
        this.updateTopicPublishInfo(topic, publishInfo);

        TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
        log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
        this.topicRouteTable.put(topic, cloneTopicRouteData);

        return true;
    }

    private boolean updateSubscribeInfoTable(TopicRouteData topicRouteData, String topic) {
        final TopicRouteData tmp = new TopicRouteData(topicRouteData);
        tmp.setTopicQueueMappingByBroker(null);
        Set<MessageQueue> newSubscribeInfo = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, tmp);
        Set<MessageQueue> oldSubscribeInfo = topicSubscribeInfoTable.get(topic);

        if (Objects.equals(newSubscribeInfo, oldSubscribeInfo)) {
            return false;
        }

        log.info("the topic[{}] subscribe message queue changed, old[{}] ,new[{}]", topic, oldSubscribeInfo, newSubscribeInfo);
        topicSubscribeInfoTable.put(topic, newSubscribeInfo);

        brokerController.getPopRebalanceCacheManager().removeTopicCache(topic);

        return true;

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        final TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);
        return null == prev || !prev.ok();
    }

    private void cleanNoneRouteTopic(String topic) {
        // clean no used topic
        topicSubscribeInfoTable.remove(topic);
    }

    private void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    public void shutdown() {
        if (null != this.scheduledExecutorService) {
            this.scheduledExecutorService.shutdown();
        }
    }

    public TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            this.updateTopicRouteInfoFromNameServer(topic, true, false);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        return topicPublishInfo;
    }

    public String findBrokerAddressInPublish(String brokerName) {
        if (brokerName == null) {
            return null;
        }
        Map<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public String findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        if (brokerName == null) {
            return null;
        }
        String brokerAddr = null;
        boolean found = false;

        Map<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            boolean slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }

            if (!found && !onlyThisBroker) {
                Map.Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                found = true;
            }
        }

        return brokerAddr;

    }

    public Set<MessageQueue> getTopicSubscribeInfo(String topic) {
        Set<MessageQueue> queues = topicSubscribeInfoTable.get(topic);
        if (null == queues || queues.isEmpty()) {
            this.updateTopicRouteInfoFromNameServer(topic, false, true);
            queues = this.topicSubscribeInfoTable.get(topic);
        }
        return queues;
    }
}
