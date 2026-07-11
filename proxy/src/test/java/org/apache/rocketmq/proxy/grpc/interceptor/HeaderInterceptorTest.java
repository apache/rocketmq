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

package org.apache.rocketmq.proxy.grpc.interceptor;

import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import java.net.InetSocketAddress;
import org.apache.rocketmq.common.constant.HAProxyConstants;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.grpc.constant.AttributeKeys;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HeaderInterceptorTest {

    @Test
    public void transportMetadataReplacesClientSuppliedAddressesAndChannelId() {
        @SuppressWarnings("unchecked")
        ServerCall<QueryRouteRequest, QueryRouteResponse> call = mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<QueryRouteRequest, QueryRouteResponse> next = mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<QueryRouteRequest> listener = mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.REMOTE_ADDRESS, "203.0.113.10:9999");
        headers.put(GrpcConstants.LOCAL_ADDRESS, "203.0.113.11:9998");
        headers.put(GrpcConstants.CHANNEL_ID, "forged-channel");
        Attributes attributes = Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, new InetSocketAddress("192.0.2.10", 8080))
            .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, new InetSocketAddress("127.0.0.1", 8081))
            .set(AttributeKeys.CHANNEL_ID, "transport-channel")
            .build();
        when(call.getAttributes()).thenReturn(attributes);
        when(next.startCall(call, headers)).thenReturn(listener);

        ServerCall.Listener<QueryRouteRequest> result =
            new HeaderInterceptor().interceptCall(call, headers, next);

        assertThat(result).isSameAs(listener);
        assertThat(headers.get(GrpcConstants.REMOTE_ADDRESS)).isEqualTo("192.0.2.10:8080");
        assertThat(headers.get(GrpcConstants.LOCAL_ADDRESS)).isEqualTo("127.0.0.1:8081");
        assertThat(headers.get(GrpcConstants.CHANNEL_ID)).isEqualTo("transport-channel");
    }

    @Test
    public void missingTransportMetadataClearsClientSuppliedAddressesAndChannelId() {
        @SuppressWarnings("unchecked")
        ServerCall<QueryRouteRequest, QueryRouteResponse> call = mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<QueryRouteRequest, QueryRouteResponse> next = mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<QueryRouteRequest> listener = mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.REMOTE_ADDRESS, "203.0.113.10:9999");
        headers.put(GrpcConstants.LOCAL_ADDRESS, "203.0.113.11:9998");
        headers.put(GrpcConstants.CHANNEL_ID, "forged-channel");
        when(call.getAttributes()).thenReturn(Attributes.EMPTY);
        when(next.startCall(call, headers)).thenReturn(listener);

        ServerCall.Listener<QueryRouteRequest> result =
            new HeaderInterceptor().interceptCall(call, headers, next);

        assertThat(result).isSameAs(listener);
        assertThat(headers.get(GrpcConstants.REMOTE_ADDRESS)).isNull();
        assertThat(headers.get(GrpcConstants.LOCAL_ADDRESS)).isNull();
        assertThat(headers.get(GrpcConstants.CHANNEL_ID)).isNull();
    }

    @Test
    public void missingTransportMetadataClearsClientSuppliedProxyProtocolHeaders() {
        @SuppressWarnings("unchecked")
        ServerCall<QueryRouteRequest, QueryRouteResponse> call = mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<QueryRouteRequest, QueryRouteResponse> next = mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<QueryRouteRequest> listener = mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();
        Metadata.Key<String> proxyAddress = asciiKey(HAProxyConstants.PROXY_PROTOCOL_ADDR);
        Metadata.Key<String> proxyPort = asciiKey(HAProxyConstants.PROXY_PROTOCOL_PORT);
        Metadata.Key<String> proxyServerAddress = asciiKey(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR);
        Metadata.Key<String> proxyServerPort = asciiKey(HAProxyConstants.PROXY_PROTOCOL_SERVER_PORT);
        Metadata.Key<String> proxyTlv = asciiKey(HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + "01");
        headers.put(proxyAddress, "203.0.113.20");
        headers.put(proxyPort, "9000");
        headers.put(proxyServerAddress, "203.0.113.21");
        headers.put(proxyServerPort, "9001");
        headers.put(proxyTlv, "forged-tlv");
        when(call.getAttributes()).thenReturn(Attributes.EMPTY);
        when(next.startCall(call, headers)).thenReturn(listener);

        new HeaderInterceptor().interceptCall(call, headers, next);

        assertThat(headers.get(proxyAddress)).isNull();
        assertThat(headers.get(proxyPort)).isNull();
        assertThat(headers.get(proxyServerAddress)).isNull();
        assertThat(headers.get(proxyServerPort)).isNull();
        assertThat(headers.get(proxyTlv)).isNull();
    }

    @Test
    public void transportProxyProtocolMetadataReplacesOnlyTrustedHeaders() {
        @SuppressWarnings("unchecked")
        ServerCall<QueryRouteRequest, QueryRouteResponse> call = mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<QueryRouteRequest, QueryRouteResponse> next = mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<QueryRouteRequest> listener = mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();
        Metadata.Key<String> proxyAddress = asciiKey(HAProxyConstants.PROXY_PROTOCOL_ADDR);
        Metadata.Key<String> proxyPort = asciiKey(HAProxyConstants.PROXY_PROTOCOL_PORT);
        Metadata.Key<String> proxyServerAddress = asciiKey(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR);
        Metadata.Key<String> proxyTlv = asciiKey(HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + "01");
        headers.put(proxyAddress, "203.0.113.20");
        headers.put(proxyPort, "9000");
        headers.put(proxyServerAddress, "203.0.113.21");
        headers.put(proxyTlv, "forged-tlv");
        Attributes.Key<String> proxyTlvAttribute =
            Attributes.Key.create(HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + "01");
        Attributes attributes = Attributes.newBuilder()
            .set(AttributeKeys.PROXY_PROTOCOL_ADDR, "192.0.2.20")
            .set(AttributeKeys.PROXY_PROTOCOL_PORT, "8080")
            .set(proxyTlvAttribute, "trusted-tlv")
            .build();
        when(call.getAttributes()).thenReturn(attributes);
        when(next.startCall(call, headers)).thenReturn(listener);

        new HeaderInterceptor().interceptCall(call, headers, next);

        assertThat(headers.get(proxyAddress)).isEqualTo("192.0.2.20");
        assertThat(headers.get(proxyPort)).isEqualTo("8080");
        assertThat(headers.get(proxyServerAddress)).isNull();
        assertThat(headers.get(proxyTlv)).isEqualTo("trusted-tlv");
    }

    @Test
    public void missingTransportMetadataClearsClientSuppliedBinaryProxyProtocolHeader() {
        @SuppressWarnings("unchecked")
        ServerCall<QueryRouteRequest, QueryRouteResponse> call = mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<QueryRouteRequest, QueryRouteResponse> next = mock(ServerCallHandler.class);
        @SuppressWarnings("unchecked")
        ServerCall.Listener<QueryRouteRequest> listener = mock(ServerCall.Listener.class);
        Metadata headers = new Metadata();
        Metadata.Key<byte[]> binaryProxyTlv = Metadata.Key.of(
            HAProxyConstants.PROXY_PROTOCOL_TLV_PREFIX + "01-bin",
            Metadata.BINARY_BYTE_MARSHALLER);
        headers.put(binaryProxyTlv, new byte[] {1, 2, 3});
        when(call.getAttributes()).thenReturn(Attributes.EMPTY);
        when(next.startCall(call, headers)).thenReturn(listener);

        new HeaderInterceptor().interceptCall(call, headers, next);

        assertThat(headers.get(binaryProxyTlv)).isNull();
    }

    private static Metadata.Key<String> asciiKey(String name) {
        return Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
    }
}
