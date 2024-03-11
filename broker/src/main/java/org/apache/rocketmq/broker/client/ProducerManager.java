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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.body.ProducerInfo;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private static final int GET_AVAILABLE_CHANNEL_RETRY_COUNT = 3;
    private final ConcurrentHashMap<String /* group name */, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();
    protected final BrokerStatsManager brokerStatsManager;
    private final PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();
    private final List<ProducerChangeListener> producerChangeListenerList = new CopyOnWriteArrayList<>();

    public ProducerManager() {
        this.brokerStatsManager = null;
    }

    public ProducerManager(final BrokerStatsManager brokerStatsManager) {
        this.brokerStatsManager = brokerStatsManager;
    }

    public int groupSize() {
        return this.groupChannelTable.size();
    }

    public boolean groupOnline(String group) {
        Map<Channel, ClientChannelInfo> channels = this.groupChannelTable.get(group);
        return channels != null && !channels.isEmpty();
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        return groupChannelTable;
    }

    public ProducerTableInfo getProducerTable() {
        Map<String, List<ProducerInfo>> map = new HashMap<>();
        for (String group : this.groupChannelTable.keySet()) {
            for (Entry<Channel, ClientChannelInfo> entry: this.groupChannelTable.get(group).entrySet()) {
                ClientChannelInfo clientChannelInfo = entry.getValue();
                if (map.containsKey(group)) {
                    map.get(group).add(new ProducerInfo(
                            clientChannelInfo.getClientId(),
                            clientChannelInfo.getChannel().remoteAddress().toString(),
                            clientChannelInfo.getLanguage(),
                            clientChannelInfo.getVersion(),
                            clientChannelInfo.getLastUpdateTimestamp()
                    ));
                } else {
                    map.put(group, new ArrayList<>(Collections.singleton(new ProducerInfo(
                        clientChannelInfo.getClientId(),
                        clientChannelInfo.getChannel().remoteAddress().toString(),
                        clientChannelInfo.getLanguage(),
                        clientChannelInfo.getVersion(),
                        clientChannelInfo.getLastUpdateTimestamp()
                    ))));
                }
            }
        }
        return new ProducerTableInfo(map);
    }

    public void scanNotActiveChannel() {
        Iterator<Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> iterator = this.groupChannelTable.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = iterator.next();

            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                // final Integer id = item.getKey();
                final ClientChannelInfo info = item.getValue();

                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    it.remove();
                    Channel channelInClientTable = clientChannelTable.get(info.getClientId());
                    if (channelInClientTable != null && channelInClientTable.equals(info.getChannel())) {
                        clientChannelTable.remove(info.getClientId());
                    }
                    log.warn(
                            "ProducerManager#scanNotActiveChannel: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                            RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, info);
                    RemotingHelper.closeChannel(info.getChannel());
                }
            }

            if (chlMap.isEmpty()) {
                log.warn("SCAN: remove expired channel from ProducerManager groupChannelTable, all clear, group={}", group);
                iterator.remove();
                callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
            }
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        AtomicBoolean removed = new AtomicBoolean(false);
        if (channel != null) {
            for (final Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                    .entrySet()) {
                final String group = entry.getKey();
                final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable = entry.getValue();
                final ClientChannelInfo clientChannelInfo = clientChannelInfoTable.get(channel);
                if (clientChannelInfo != null) {
                    this.groupChannelTable.computeIfPresent(group, (key, channelTable) -> {
                        if (channelTable.remove(channel) == null) {
                            return channelTable;
                        }
                        // clientId may have registered a new channel in another groupId
                        clientChannelTable.remove(clientChannelInfo.getClientId(), channel);
                        removed.set(true);
                        log.info("NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                clientChannelInfo.toString(), remoteAddr, group);
                        callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo);
                        if (channelTable.isEmpty()) {
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                            callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
                            return null;
                        }
                        return channelTable;
                    });
                    break;
                }
            }
        }
        return removed.get();
    }

    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        this.groupChannelTable.compute(group, (keyGroup, channelTable) -> {
            if (channelTable == null) {
                channelTable = new ConcurrentHashMap<>();
            }
            ClientChannelInfo clientChannelInfoFound = channelTable.computeIfAbsent(clientChannelInfo.getChannel(), keyChannel -> {
                clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
                log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
                return clientChannelInfo;
            });
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
            return channelTable;
        });
    }

    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        this.groupChannelTable.computeIfPresent(group, (key, channelTable) -> {
            if (channelTable.remove(clientChannelInfo.getChannel()) == null) {
                return channelTable;
            }
            // clientId may have registered a new channel in another groupId
            clientChannelTable.remove(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                    clientChannelInfo.toString());
            callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo);
            if (channelTable.isEmpty()) {
                log.info("unregister a producer group[{}] from groupChannelTable", group);
                callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
                return null;
            }
            return channelTable;
        });
    }

    public Channel getAvailableChannel(String groupId) {
        if (groupId == null) {
            return null;
        }
        List<Channel> channelList;
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        if (channelClientChannelInfoHashMap != null) {
            if (channelClientChannelInfoHashMap.size() == 0) {
                log.warn("Channel list is empty. groupId={}", groupId);
                return null;
            }
            channelList = new ArrayList<>(channelClientChannelInfoHashMap.keySet());
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }

        int size = channelList.size();

        Channel lastActiveChannel = null;

        int count = 0;
        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel;
        boolean isOk;
        while (count++ < GET_AVAILABLE_CHANNEL_RETRY_COUNT) {
            channel = channelList.get(index);
            isOk = channel.isActive() && channel.isWritable();
            if (isOk) {
                return channel;
            }
            if (channel.isActive()) {
                lastActiveChannel = channel;
            }
            index = (++index) % size;
        }

        return lastActiveChannel;
    }

    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }

    private void callProducerChangeListener(ProducerGroupEvent event, String group,
        ClientChannelInfo clientChannelInfo) {
        for (ProducerChangeListener listener : producerChangeListenerList) {
            try {
                listener.handle(event, group, clientChannelInfo);
            } catch (Throwable t) {
                log.error("err when call producerChangeListener", t);
            }
        }
    }

    public void appendProducerChangeListener(ProducerChangeListener producerChangeListener) {
        producerChangeListenerList.add(producerChangeListener);
    }
}
