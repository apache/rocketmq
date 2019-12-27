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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class ProducerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;
    private final Lock groupChannelLock = new ReentrantLock();
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
    }

    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    Iterator<Map.Entry<String, HashMap<Channel, ClientChannelInfo>>> iter = groupChannelTable.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry = iter.next();
                        String key = entry.getKey();
                        HashMap<Channel, ClientChannelInfo> val = entry.getValue();
                        HashMap<Channel, ClientChannelInfo> tmp = new HashMap<Channel, ClientChannelInfo>();
                        tmp.putAll(val);
                        newGroupChannelTable.put(key, tmp);
                    }
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }

    public void scanNotActiveChannel() {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                        .entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();

                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                it.remove();
                                clientChannelTable.remove(info.getClientId());
                                log.warn(
                                    "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                    RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                            final String group = entry.getKey();
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                entry.getValue();
                            final ClientChannelInfo clientChannelInfo =
                                clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                                clientChannelTable.remove(clientChannelInfo.getClientId());
                                log.info(
                                    "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                    clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                        this.groupChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        channelTable = new HashMap<>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
                        log.info("new producer connected, group: {} channel: {}", group,
                            clientChannelInfo.toString());
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        clientChannelTable.remove(clientChannelInfo.getClientId());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public Channel getAvaliableChannel(String groupId) {
        HashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        List<Channel> channelList = new ArrayList<Channel>();
        if (channelClientChannelInfoHashMap != null) {
            for (Channel channel : channelClientChannelInfoHashMap.keySet()) {
                channelList.add(channel);
            }
            int size = channelList.size();
            if (0 == size) {
                log.warn("Channel list is empty. groupId={}", groupId);
                return null;
            }

            int index = positiveAtomicCounter.incrementAndGet() % size;
            Channel channel = channelList.get(index);
            int count = 0;
            boolean isOk = channel.isActive() && channel.isWritable();
            while (count++ < GET_AVALIABLE_CHANNEL_RETRY_COUNT) {
                if (isOk) {
                    return channel;
                }
                index = (++index) % size;
                channel = channelList.get(index);
                isOk = channel.isActive() && channel.isWritable();
            }
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }
        return null;
    }

    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }
}
