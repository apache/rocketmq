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
package org.apache.rocketmq.snode.client;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * TODO Refactor this manager
 */
public class ConsumerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ConcurrentMap<String/*Group*/, ConsumerGroupInfo> consumerTable = new ConcurrentHashMap<>(1024);

    private final ConcurrentHashMap<String/*Topic*/, ConcurrentHashMap<Integer/*QueueId*/, ConcurrentHashMap<String/*ConsumerGroup*/, ClientChannelInfo>>> topicConsumerTable = new ConcurrentHashMap<>(2048);

    private final ConsumerIdsChangeListener consumerIdsChangeListener;

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }

    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    private void clearPushSession(final String consumerGroup, final ConsumerGroupInfo info,
        final RemotingChannel channel) {
        Set<SubscriptionData> subscriptionDataSet = info.getSubscriotionDataSet(channel);
        removeConsumerTopicTable(consumerGroup, subscriptionDataSet, channel);
    }

    public void doChannelCloseEvent(final String remoteAddr, final RemotingChannel channel) {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            clearPushSession(info.getGroupName(), info, channel);
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        log.info("Unregister consumer ok, no any connection, and remove consumer group, {}", next.getKey());
                        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
    }

    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {

        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        boolean r1 = consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
            consumeFromWhere);

        boolean r2 = consumerGroupInfo.updateSubscription(subList);

        consumerGroupInfo.updateChannelSubscription(clientChannelInfo, subList);

        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable) {
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);

        return r1 || r2;
    }

    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            consumerGroupInfo.removeChannelSubscription(clientChannelInfo.getChannel());
            clearPushSession(group, consumerGroupInfo, clientChannelInfo.getChannel());
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    log.info("Unregister consumer ok, no any connection, and remove consumer group, {}", group);

                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.UNREGISTER, group);
                }
            }
            if (isNotifyConsumerIdsChangedEnable) {
                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentMap<RemotingChannel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<RemotingChannel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<RemotingChannel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                        "SCAN: Remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel().remoteAddress()), group);
                    clientChannelInfo.getChannel().close();
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                log.warn(
                    "SCAN: Remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
        }
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

    public void registerPushSession(String consumerGroup, Set<SubscriptionData> subscriptionDataSet,
        ClientChannelInfo clientChannelInfo) {
        if (clientChannelInfo != null) {
            for (SubscriptionData subscriptionData : subscriptionDataSet) {
                String topic = subscriptionData.getTopic();
                for (Integer queueId : subscriptionData.getQueueIdSet()) {
                    ConcurrentHashMap<Integer, ConcurrentHashMap<String, ClientChannelInfo>> clientChannelInfoMap = this.topicConsumerTable.get(topic);
                    if (clientChannelInfoMap == null) {
                        clientChannelInfoMap = new ConcurrentHashMap<>();
                        ConcurrentHashMap prev = this.topicConsumerTable.putIfAbsent(topic, clientChannelInfoMap);
                        if (prev != null) {
                            clientChannelInfoMap = prev;
                        }
                    }
                    log.info("Register push for consumer group: {} topic: {}, queueId: {}", consumerGroup, topic, queueId);
                    ConcurrentHashMap<String, ClientChannelInfo> consumerGroupChannelTable = clientChannelInfoMap.get(queueId);
                    if (consumerGroupChannelTable == null) {
                        consumerGroupChannelTable = new ConcurrentHashMap<>();
                        ConcurrentHashMap<String, ClientChannelInfo> preMap = clientChannelInfoMap.putIfAbsent(queueId, consumerGroupChannelTable);
                        if (preMap != null) {
                            consumerGroupChannelTable = preMap;
                        }
                    }
                    consumerGroupChannelTable.putIfAbsent(consumerGroup, clientChannelInfo);
                    clientChannelInfoMap.put(queueId, consumerGroupChannelTable);
                }
            }
        }
    }

    public ConcurrentHashMap getClientInfoTable(String topic, Integer queueId) {
        ConcurrentHashMap<Integer, ConcurrentHashMap<String, ClientChannelInfo>> clientChannelInfoMap = this.topicConsumerTable.get(topic);
        if (clientChannelInfoMap != null) {
            return clientChannelInfoMap.get(queueId);
        }
        return null;
    }

    public void removeConsumerTopicTable(String consumerGroup, Set<SubscriptionData> subscriptionDataSet,
        RemotingChannel remotingChannel) {
        if (subscriptionDataSet != null) {
            for (SubscriptionData subscriptionData : subscriptionDataSet) {
                String topic = subscriptionData.getTopic();
                for (Integer queueId : subscriptionData.getQueueIdSet()) {
                    ConcurrentHashMap<Integer, ConcurrentHashMap<String, ClientChannelInfo>> clientChannelInfoMap = this.topicConsumerTable.get(topic);
                    if (clientChannelInfoMap != null) {
                        ConcurrentHashMap<String, ClientChannelInfo> queueConsumerGroupMap = clientChannelInfoMap.get(queueId);
                        if (queueConsumerGroupMap != null) {
                            ClientChannelInfo clientChannelInfo = queueConsumerGroupMap.get(consumerGroup);
                            if (clientChannelInfo.getChannel().equals(remotingChannel)) {
                                log.info("Remove push topic: {}, queueId: {}, consumerGroup:{} session", topic, queueId, consumerGroup);
                                queueConsumerGroupMap.remove(consumerGroup, clientChannelInfo);
                            }
                        }
                        if (clientChannelInfoMap.isEmpty()) {
                            log.info("All consumer offline, so remove this map");
                            this.topicConsumerTable.remove(topic, clientChannelInfoMap);
                        }
                    }
                }
            }
        }
    }
}
