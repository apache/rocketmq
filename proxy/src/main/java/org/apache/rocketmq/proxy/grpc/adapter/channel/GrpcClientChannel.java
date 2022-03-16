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
package org.apache.rocketmq.proxy.grpc.adapter.channel;

import apache.rocketmq.v1.PollCommandResponse;
import io.netty.channel.ChannelFuture;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.channel.SimpleChannel;

public class GrpcClientChannel extends SimpleChannel {

    private static final Map<String /* group */, List<String>/* clientId */> GROUP_CLIENT_IDS = new ConcurrentHashMap<>();

    private final AtomicReference<CompletableFuture<PollCommandResponse>> pollCommandResponseFutureRef = new AtomicReference<>();

    public GrpcClientChannel() {
        super(ChannelManager.createSimpleChannelDirectly());
    }

    public static GrpcClientChannel create(ChannelManager channelManager, String group, String clientId) {
        GrpcClientChannel channel = channelManager.createChannel(
            buildKey(group, clientId),
            GrpcClientChannel::new,
            GrpcClientChannel.class);

        GROUP_CLIENT_IDS.compute(group, (groupKey, clientIds) -> {
            if (clientIds == null) {
                clientIds = new CopyOnWriteArrayList<>();
            }
            clientIds.add(clientId);
            return clientIds;
        });
        return channel;
    }

    public static void addClientObserver(ChannelManager channelManager, String group, String clientId, CompletableFuture<PollCommandResponse> future) {
        GrpcClientChannel channel = getChannel(channelManager, group, clientId);
        channel.pollCommandResponseFutureRef.set(future);
    }

    public static GrpcClientChannel getChannel(ChannelManager channelManager, String group, String clientId) {
        return channelManager.getChannel(buildKey(group, clientId), GrpcClientChannel.class);
    }

    public static GrpcClientChannel removeChannel(ChannelManager channelManager, String group, String clientId) {
        return channelManager.removeChannel(buildKey(group, clientId), GrpcClientChannel.class);
    }

    private static String buildKey(String group, String clientId) {
        return group + "@" + clientId;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        CompletableFuture<PollCommandResponse> future = pollCommandResponseFutureRef.get();
        if (msg instanceof PollCommandResponse) {
            PollCommandResponse response = (PollCommandResponse) msg;
            future.complete(response);
        }
        return super.writeAndFlush(msg);
    }
}
