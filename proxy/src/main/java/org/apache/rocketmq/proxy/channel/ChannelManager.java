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

package org.apache.rocketmq.proxy.channel;

import com.google.common.base.Strings;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.common.Cleaner;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);
    private final ConcurrentMap<String, SimpleChannel> clientIdChannelMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String /* group */, Set<String>/* clientId */> groupClientIdMap = new ConcurrentHashMap<>();

    public SimpleChannel createChannel() {
        return createChannel(anonymousChannelId());
    }

    public SimpleChannel createChannel(String clientId) {
        return createChannel(clientId, ChannelManager::createSimpleChannelDirectly, SimpleChannel.class);
    }

    public <T extends SimpleChannel> T createChannel(Supplier<T> creator, Class<T> clazz) {
        return createChannel(anonymousChannelId(clazz.getName()), creator, clazz);
    }

    public <T extends SimpleChannel> T createChannel(String clientId, Supplier<T> creator, Class<T> clazz) {
        if (Strings.isNullOrEmpty(clientId)) {
            LOGGER.warn("ClientId is unexpected null or empty");
            return creator.get();
        }

        if (!clientIdChannelMap.containsKey(clientId)) {
            clientIdChannelMap.putIfAbsent(clientId, creator.get());
        }

        T channel = clazz.cast(clientIdChannelMap.get(clientId));
        channel.updateLastAccessTime();
        return channel;
    }

    public <T extends SimpleChannel> T getChannel(String clientId, Class<T> clazz) {
        SimpleChannel channel = clientIdChannelMap.get(clientId);
        if (channel == null) {
            return null;
        }
        return clazz.cast(channel);
    }

    public <T extends SimpleChannel> void setChannel(String clientId, T channel) {
        clientIdChannelMap.put(clientId, channel);
    }

    public <T extends SimpleChannel> T removeChannel(String clientId, Class<T> clazz) {
        SimpleChannel channel = clientIdChannelMap.remove(clientId);
        if (channel == null) {
            return null;
        }
        return clazz.cast(channel);
    }

    private String anonymousChannelId() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return clientHost + "@" + localAddress;
    }

    private String anonymousChannelId(String className) {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return className + "@" + clientHost + "@" + localAddress;
    }

    public static SimpleChannel createSimpleChannelDirectly() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current())
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return new SimpleChannel(null, clientHost, localAddress, ConfigurationManager.getProxyConfig().getChannelExpiredInSeconds());
    }

    public void addGroupClientId(String group, String clientId) {
        groupClientIdMap.computeIfAbsent(group, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()))
            .add(clientId);
    }

    public List<String> getClientIdList(String group) {
        return new ArrayList<>(groupClientIdMap.get(group));
    }

    /**
     * Scan and remove inactive mocking channels; Scan and clean expired requests;
     */
    public void scanAndCleanChannels() {
        try {
            Iterator<Map.Entry<String, SimpleChannel>> iterator = clientIdChannelMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SimpleChannel> entry = iterator.next();
                if (!entry.getValue().isActive()) {
                    iterator.remove();
                    if (entry.getValue() instanceof GrpcClientChannel) {
                        GrpcClientChannel grpcClientChannel = (GrpcClientChannel) entry.getValue();
                        groupClientIdMap.computeIfPresent(grpcClientChannel.getGroup(), (group, clientIds) -> {
                            clientIds.remove(grpcClientChannel.getClientId());
                            if (clientIds.isEmpty()) {
                                return null;
                            }
                            return clientIds;
                        });
                    }
                } else {
                    if (entry.getValue() instanceof Cleaner) {
                        Cleaner cleaner = (Cleaner) entry.getValue();
                        cleaner.clean();
                    }
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Unexpected exception", e);
        }
    }
}
