/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.Code;
import com.google.protobuf.StringValue;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminPeerGrpcTransport implements ProxyClientAdminPeerMessageTransport {
    private final Map<String, Channel> channels;
    private final List<String> proxyIds;
    private final Invoker invoker;
    private final ProxyClientAdminPeerMessageCodec codec;

    public ProxyClientAdminPeerGrpcTransport(Map<String, Channel> channels) {
        this(channels, new BlockingUnaryInvoker());
    }

    ProxyClientAdminPeerGrpcTransport(Map<String, Channel> channels, Invoker invoker) {
        this(channels, invoker, ProxyClientAdminPeerMessageCodec.getInstance());
    }

    ProxyClientAdminPeerGrpcTransport(Map<String, Channel> channels, Invoker invoker,
        ProxyClientAdminPeerMessageCodec codec) {
        if (channels == null) {
            throw new IllegalArgumentException("channels is required");
        }
        if (invoker == null) {
            throw new IllegalArgumentException("invoker is required");
        }
        if (codec == null) {
            throw new IllegalArgumentException("codec is required");
        }
        TreeMap<String, Channel> sortedChannels = new TreeMap<>();
        for (Map.Entry<String, Channel> entry : channels.entrySet()) {
            String proxyId = requireProxyId(entry.getKey());
            if (sortedChannels.containsKey(proxyId)) {
                throw new IllegalArgumentException("Duplicate proxyId: " + proxyId);
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("channel is required");
            }
            sortedChannels.put(proxyId, entry.getValue());
        }
        if (sortedChannels.isEmpty()) {
            throw new IllegalArgumentException("at least one channel is required");
        }
        this.channels = Collections.unmodifiableMap(new LinkedHashMap<>(sortedChannels));
        this.proxyIds = Collections.unmodifiableList(new ArrayList<>(sortedChannels.keySet()));
        this.invoker = invoker;
        this.codec = codec;
    }

    @Override
    public List<String> listProxyIds() {
        return this.proxyIds;
    }

    @Override
    public String execute(ProxyContext ctx, String proxyId, String requestMessage) {
        String requiredProxyId = requireProxyId(proxyId);
        Channel channel = this.channels.get(requiredProxyId);
        if (channel == null) {
            return this.encodeError(requiredProxyId, Code.NOT_FOUND, "Proxy not found: " + requiredProxyId);
        }
        try {
            String responseMessage = StringUtils.trimToNull(this.invoker.execute(channel, requestMessage));
            if (responseMessage == null) {
                return this.encodeError(requiredProxyId, Code.INTERNAL_SERVER_ERROR,
                    "peer response message is required");
            }
            return responseMessage;
        } catch (Throwable t) {
            return this.encodeError(
                requiredProxyId,
                Code.INTERNAL_SERVER_ERROR,
                StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
            );
        }
    }

    private String encodeError(String proxyId, Code code, String message) {
        ProxyClientAdminPeerResponse<ProxyClientPage> response = ProxyClientAdminPeerResponse.error(
            proxyId,
            code.name(),
            message
        );
        return this.codec.encodePageResponse(response);
    }

    private static String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }

    @FunctionalInterface
    interface Invoker {
        String execute(Channel channel, String requestMessage);
    }

    private static class BlockingUnaryInvoker implements Invoker {
        @Override
        public String execute(Channel channel, String requestMessage) {
            StringValue response = ClientCalls.blockingUnaryCall(
                channel,
                ProxyClientAdminPeerGrpcService.EXECUTE_METHOD,
                CallOptions.DEFAULT,
                StringValue.of(StringUtils.defaultString(requestMessage))
            );
            return response.getValue();
        }
    }
}
