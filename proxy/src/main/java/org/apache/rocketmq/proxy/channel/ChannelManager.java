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

import io.grpc.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;

public class ChannelManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final ConcurrentMap<String /* clientId */, SimpleChannel> clientIdChannelMap = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String /* group */, Set<String>/* clientId */> groupClientIdMap = new ConcurrentHashMap<>();

    public SimpleChannel createChannel(Context ctx) {
        return createChannel(ctx, anonymousChannelId(ctx));
    }

    public SimpleChannel createChannel(Context ctx, String clientId) {
        return createChannel(ctx, clientId, ChannelManager::createSimpleChannelDirectly, SimpleChannel.class);
    }

    public <T extends SimpleChannel> T createChannel(Context ctx, Function<Context, T> creator, Class<T> clazz) {
        return createChannel(ctx, anonymousChannelId(ctx, clazz.getName()), creator, clazz);
    }

    public <T extends SimpleChannel> T createChannel(Context ctx, String clientId, Function<Context, T> creator, Class<T> clazz) {
        if (StringUtils.isBlank(clientId)) {
            log.warn("ClientId is unexpected null or empty");
            return creator.apply(ctx);
        }

        clientIdChannelMap.computeIfAbsent(clientId, key -> creator.apply(ctx));

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

    private String anonymousChannelId(Context ctx) {
        final String clientHost = InterceptorConstants.METADATA.get(ctx)
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(ctx)
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return clientHost + "@" + localAddress;
    }

    private String anonymousChannelId(Context ctx, String className) {
        final String clientHost = InterceptorConstants.METADATA.get(ctx)
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(ctx)
            .get(InterceptorConstants.LOCAL_ADDRESS);
        return className + "@" + clientHost + "@" + localAddress;
    }

    public static SimpleChannel createSimpleChannelDirectly(Context ctx) {
        final String clientHost = InterceptorConstants.METADATA.get(ctx)
            .get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(ctx)
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

    public void onClientOffline(String clientId) {
        SimpleChannel simpleChannel = clientIdChannelMap.remove(clientId);
        if (simpleChannel == null) {
            return;
        }
        if (simpleChannel instanceof GrpcClientChannel) {
            GrpcClientChannel grpcClientChannel = (GrpcClientChannel) simpleChannel;
            groupClientIdMap.computeIfPresent(grpcClientChannel.getGroup(), (group, clientIds) -> {
                clientIds.remove(grpcClientChannel.getClientId());
                if (clientIds.isEmpty()) {
                    return null;
                }
                return clientIds;
            });
        }
    }
}
