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

package org.apache.rocketmq.proxy.remoting.channel;

import io.netty.channel.Channel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.remoting.RemotingProxyOutClient;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemotingChannelManager implements StartAndShutdown {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ProxyRelayService proxyRelayService;
    protected final ConcurrentMap<String /* group */, Map<Channel /* raw channel */, RemotingChannel>> groupChannelMap = new ConcurrentHashMap<>();

    private final RemotingProxyOutClient remotingProxyOutClient;

    public RemotingChannelManager(RemotingProxyOutClient remotingProxyOutClient, ProxyRelayService proxyRelayService) {
        this.remotingProxyOutClient = remotingProxyOutClient;
        this.proxyRelayService = proxyRelayService;
    }

    protected String buildProducerKey(String group) {
        return buildKey("p", group);
    }

    protected String buildConsumerKey(String group) {
        return buildKey("c", group);
    }

    protected String buildKey(String prefix, String group) {
        return prefix + group;
    }

    protected String getGroupFromKey(String key) {
        return key.substring(1);
    }

    public RemotingChannel createProducerChannel(Channel channel, String group, String clientId) {
        return createChannel(channel, buildProducerKey(group), clientId, Collections.emptySet());
    }

    public RemotingChannel createConsumerChannel(Channel channel, String group, String clientId, Set<SubscriptionData> subscriptionData) {
        return createChannel(channel, buildConsumerKey(group), clientId, subscriptionData);
    }

    protected RemotingChannel createChannel(Channel channel, String group, String clientId, Set<SubscriptionData> subscriptionData) {
        this.groupChannelMap.compute(group, (groupKey, clientIdMap) -> {
            if (clientIdMap == null) {
                clientIdMap = new ConcurrentHashMap<>();
            }
            clientIdMap.computeIfAbsent(channel, clientIdKey -> new RemotingChannel(remotingProxyOutClient, proxyRelayService, channel, clientId, subscriptionData));
            return clientIdMap;
        });
        return getChannel(group, channel);
    }

    public RemotingChannel getConsumerChannel(String group, Channel channel) {
        return getChannel(buildConsumerKey(group), channel);
    }

    public RemotingChannel getProducerChannel(String group, Channel channel) {
        return getChannel(buildProducerKey(group), channel);
    }

    protected RemotingChannel getChannel(String group, Channel channel) {
        Map<Channel, RemotingChannel> clientIdChannelMap = this.groupChannelMap.get(group);
        if (clientIdChannelMap == null) {
            return null;
        }
        return clientIdChannelMap.get(channel);
    }

    public Set<RemotingChannel> removeChannel(Channel channel) {
        Set<RemotingChannel> removedChannelSet = new HashSet<>();
        for (Map.Entry<String, Map<Channel /* raw channel */, RemotingChannel>> entry : groupChannelMap.entrySet()) {
            Map<Channel /* raw channel */, RemotingChannel> channelMap = entry.getValue();

            RemotingChannel remotingChannel = channelMap.remove(channel);
            if (remotingChannel != null) {
                removedChannelSet.add(remotingChannel);
            }
        }
        return removedChannelSet;
    }

    public RemotingChannel removeProducerChannel(String group, Channel channel) {
        return removeChannel(buildProducerKey(group), channel);
    }

    public RemotingChannel removeConsumerChannel(String group, Channel channel) {
        return removeChannel(buildConsumerKey(group), channel);
    }

    protected RemotingChannel removeChannel(String group, Channel channel) {
        AtomicReference<RemotingChannel> channelRef = new AtomicReference<>();

        this.groupChannelMap.computeIfPresent(group, (groupKey, channelMap) -> {
            channelRef.set(channelMap.remove(channel));
            if (channelMap.isEmpty()) {
                return null;
            }
            return channelMap;
        });
        return channelRef.get();
    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public void start() throws Exception {

    }
}
