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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ConsumerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ConcurrentMap<String, ConsumerGroupInfo> consumerTable =
        new ConcurrentHashMap<>(1024);
    private final ConcurrentMap<String, ConsumerGroupInfo> consumerCompensationTable =
        new ConcurrentHashMap<>(1024);
    private final List<ConsumerIdsChangeListener> consumerIdsChangeListenerList = new CopyOnWriteArrayList<>();
    protected final BrokerStatsManager brokerStatsManager;
    private final long channelExpiredTimeout;
    private final long subscriptionExpiredTimeout;
    private BrokerController brokerController;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener, long expiredTimeout) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = null;
        this.channelExpiredTimeout = expiredTimeout;
        this.subscriptionExpiredTimeout = expiredTimeout;
    }

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener,
        final BrokerStatsManager brokerStatsManager, BrokerConfig brokerConfig, BrokerController brokerController) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = brokerStatsManager;
        this.channelExpiredTimeout = brokerConfig.getChannelExpiredTimeout();
        this.subscriptionExpiredTimeout = brokerConfig.getSubscriptionExpiredTimeout();
        this.brokerController = brokerController;
    }

    public ClientChannelInfo findChannel(final String group, final String clientId) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    public ClientChannelInfo findChannel(final String group, final Channel channel) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(channel);
        }
        return null;
    }

    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        return findSubscriptionData(group, topic, true);
    }

    public SubscriptionData findSubscriptionData(final String group, final String topic,
        boolean fromCompensationTable) {
        ConsumerGroupInfo consumerGroupInfo = getConsumerGroupInfo(group, false);
        if (consumerGroupInfo != null) {
            SubscriptionData subscriptionData = consumerGroupInfo.findSubscriptionData(topic);
            if (subscriptionData != null) {
                return subscriptionData;
            }
        }

        if (fromCompensationTable) {
            ConsumerGroupInfo consumerGroupCompensationInfo = consumerCompensationTable.get(group);
            if (consumerGroupCompensationInfo != null) {
                return consumerGroupCompensationInfo.findSubscriptionData(topic);
            }
        }
        return null;
    }

    public ConcurrentMap<String, ConsumerGroupInfo> getConsumerTable() {
        return this.consumerTable;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return getConsumerGroupInfo(group, false);
    }

    public ConsumerGroupInfo getConsumerGroupInfo(String group, boolean fromCompensationTable) {
        ConsumerGroupInfo consumerGroupInfo = consumerTable.get(group);
        if (consumerGroupInfo == null && fromCompensationTable) {
            consumerGroupInfo = consumerCompensationTable.get(group);
        }
        return consumerGroupInfo;
    }

    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        boolean removed = false;
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            ClientChannelInfo clientChannelInfo = info.doChannelCloseEvent(remoteAddr, channel);

            brokerController.getPopRebalanceCacheManager().removeTopicCaches(info.getSubscribeTopics());

            if (clientChannelInfo != null) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, next.getKey(), clientChannelInfo, info.getSubscribeTopics());
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                            next.getKey());
                        callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }
                if (!isBroadcastMode(info.getMessageModel())) {
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
                }
            }
        }
        return removed;
    }

    // compensate consumer info for consumer without heartbeat
    public void compensateBasicConsumerInfo(String group, ConsumeType consumeType, MessageModel messageModel) {
        ConsumerGroupInfo consumerGroupInfo = consumerCompensationTable.computeIfAbsent(group, ConsumerGroupInfo::new);
        consumerGroupInfo.setConsumeType(consumeType);
        consumerGroupInfo.setMessageModel(messageModel);
    }

    // compensate subscription for pull consumer and consumer via proxy
    public void compensateSubscribeData(String group, String topic, SubscriptionData subscriptionData) {
        ConsumerGroupInfo consumerGroupInfo = consumerCompensationTable.computeIfAbsent(group, ConsumerGroupInfo::new);
        consumerGroupInfo.getSubscriptionTable().put(topic, subscriptionData);
    }

    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {
        return registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList,
            isNotifyConsumerIdsChangedEnable, true);
    }

    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable, boolean updateSubscription) {
        long start = System.currentTimeMillis();
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        boolean r1 =
            consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                consumeFromWhere);
        if (r1) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_REGISTER, group, clientChannelInfo,
                subList.stream().map(SubscriptionData::getTopic).collect(Collectors.toSet()));
        }
        boolean r2 = false;
        if (updateSubscription) {
            r2 = consumerGroupInfo.updateSubscription(subList);
        }

        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable && !isBroadcastMode(consumerGroupInfo.getMessageModel())) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }

        callConsumerIdsChangeListener(ConsumerGroupEvent.REGISTER, group, subList, clientChannelInfo);

        brokerController.getPopRebalanceCacheManager().removeTopicCaches(consumerGroupInfo.getSubscribeTopics());

        return r1 || r2;
    }

    public boolean registerConsumerWithoutSub(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere, boolean isNotifyConsumerIdsChangedEnable) {
        long start = System.currentTimeMillis();
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }
        boolean updateChannelRst = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel, consumeFromWhere);
        if (updateChannelRst && isNotifyConsumerIdsChangedEnable && !isBroadcastMode(consumerGroupInfo.getMessageModel())) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
        }
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }

        brokerController.getPopRebalanceCacheManager().removeTopicCaches(consumerGroupInfo.getSubscribeTopics());

        return updateChannelRst;
    }

    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            boolean removed = consumerGroupInfo.unregisterChannel(clientChannelInfo);

            brokerController.getPopRebalanceCacheManager().removeTopicCaches(consumerGroupInfo.getSubscribeTopics());

            if (removed) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
            }
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);

                    callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, group);
                }
            }
            if (isNotifyConsumerIdsChangedEnable && !isBroadcastMode(consumerGroupInfo.getMessageModel())) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    public void removeExpireConsumerGroupInfo() {
        List<String> removeList = new ArrayList<>();
        consumerCompensationTable.forEach((group, consumerGroupInfo) -> {
            List<String> removeTopicList = new ArrayList<>();
            ConcurrentMap<String, SubscriptionData> subscriptionTable = consumerGroupInfo.getSubscriptionTable();
            subscriptionTable.forEach((topic, subscriptionData) -> {
                long diff = System.currentTimeMillis() - subscriptionData.getSubVersion();
                if (diff > subscriptionExpiredTimeout) {
                    removeTopicList.add(topic);
                }
            });
            for (String topic : removeTopicList) {
                subscriptionTable.remove(topic);
                if (subscriptionTable.isEmpty()) {
                    removeList.add(group);
                }
            }
        });
        for (String group : removeList) {
            consumerCompensationTable.remove(group);
        }
    }

    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > channelExpiredTimeout) {
                    LOGGER.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
                    RemotingHelper.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                LOGGER.warn(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
        }
        removeExpireConsumerGroupInfo();
    }

    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            ConcurrentMap<String, SubscriptionData> subscriptionTable =
                entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }

    public void appendConsumerIdsChangeListener(ConsumerIdsChangeListener listener) {
        consumerIdsChangeListenerList.add(listener);
    }

    protected void callConsumerIdsChangeListener(ConsumerGroupEvent event, String group, Object... args) {
        for (ConsumerIdsChangeListener listener : consumerIdsChangeListenerList) {
            try {
                listener.handle(event, group, args);
            } catch (Throwable t) {
                LOGGER.error("err when call consumerIdsChangeListener", t);
            }
        }
    }

    private boolean isBroadcastMode(final MessageModel messageModel) {
        return MessageModel.BROADCASTING.equals(messageModel);
    }
}
