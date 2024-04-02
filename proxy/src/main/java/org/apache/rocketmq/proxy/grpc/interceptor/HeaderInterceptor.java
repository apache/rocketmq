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

package org.apache.rocketmq.proxy.grpc.interceptor;

import com.google.common.net.HostAndPort;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.grpc.constant.AttributeKeys;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class HeaderInterceptor implements ServerInterceptor {
    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call,
        Metadata headers,
        ServerCallHandler<R, W> next
    ) {
        String remoteAddress = getProxyProtocolAddress(call.getAttributes());
        if (StringUtils.isBlank(remoteAddress)) {
            SocketAddress remoteSocketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            remoteAddress = parseSocketAddress(remoteSocketAddress);
        }
        headers.put(GrpcConstants.REMOTE_ADDRESS, remoteAddress);

        SocketAddress localSocketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR);
        String localAddress = parseSocketAddress(localSocketAddress);
        headers.put(GrpcConstants.LOCAL_ADDRESS, localAddress);

        for (Attributes.Key<?> key : call.getAttributes().keys()) {
            if (!StringUtils.startsWith(key.toString(), HAProxyConstants.PROXY_PROTOCOL_PREFIX)) {
                continue;
            }
            Metadata.Key<String> headerKey
                    = Metadata.Key.of(key.toString(), Metadata.ASCII_STRING_MARSHALLER);
            String headerValue = String.valueOf(call.getAttributes().get(key));
            headers.put(headerKey, headerValue);
        }

        String channelId = call.getAttributes().get(AttributeKeys.CHANNEL_ID);
        if (StringUtils.isNotBlank(channelId)) {
            headers.put(GrpcConstants.CHANNEL_ID, channelId);
        }

        return next.startCall(call, headers);
    }

    private String parseSocketAddress(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            return HostAndPort.fromParts(
                inetSocketAddress.getAddress()
                    .getHostAddress(),
                inetSocketAddress.getPort()
            ).toString();
        }

        return "";
    }

    private String getProxyProtocolAddress(Attributes attributes) {
        String proxyProtocolAddr = attributes.get(AttributeKeys.PROXY_PROTOCOL_ADDR);
        String proxyProtocolPort = attributes.get(AttributeKeys.PROXY_PROTOCOL_PORT);
        if (StringUtils.isBlank(proxyProtocolAddr) || StringUtils.isBlank(proxyProtocolPort)) {
            return null;
        }
        return proxyProtocolAddr + ":" + proxyProtocolPort;
    }
}
