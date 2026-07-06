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
import io.grpc.Channel;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class ProxyClientAdminPeerGrpcTransportTest {

    @Test
    public void grpcTransportListsStableProxyIdsAndInvokesTargetChannel() {
        Channel proxyBChannel = mock(Channel.class);
        Channel proxyAChannel = mock(Channel.class);
        RecordingInvoker invoker = new RecordingInvoker("{\"success\":true}");
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put(" proxy-b ", proxyBChannel);
        channels.put(" proxy-a ", proxyAChannel);
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        String responseMessage = transport.execute(proxyContext(), " proxy-b ", "{\"operation\":\"LIST_CLIENTS\"}");

        assertThat(transport.listProxyIds()).containsExactly("proxy-a", "proxy-b");
        assertThat(responseMessage).isEqualTo("{\"success\":true}");
        assertThat(invoker.channel).isSameAs(proxyBChannel);
        assertThat(invoker.requestMessage).isEqualTo("{\"operation\":\"LIST_CLIENTS\"}");
    }

    @Test
    public void grpcTransportReturnsEncodedPeerErrorForMissingTargetProxy() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            new RecordingInvoker("{\"success\":true}")
        );

        String responseMessage = transport.execute(
            proxyContext(),
            " proxy-missing ",
            "{\"operation\":\"LIST_CLIENTS\"}"
        );
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-missing");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.NOT_FOUND.name());
        assertThat(response.getErrorMessage()).contains("proxy-missing");
    }

    @Test
    public void grpcTransportMapsInvokerFailureToEncodedPeerError() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            (channel, requestMessage) -> {
                throw new IllegalStateException("boom");
            }
        );

        String responseMessage = transport.execute(proxyContext(), " proxy-a ", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
        assertThat(response.getErrorMessage()).contains("boom");
    }

    @Test
    public void grpcTransportRejectsInvalidChannelMap() {
        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcTransport(null, new RecordingInvoker("{}")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("channels is required");
        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcTransport(Collections.emptyMap(),
            new RecordingInvoker("{}")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one channel is required");

        Map<String, Channel> nullChannel = new LinkedHashMap<>();
        nullChannel.put("proxy-a", null);
        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcTransport(nullChannel, new RecordingInvoker("{}")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("channel is required");

        Map<String, Channel> duplicateChannels = new LinkedHashMap<>();
        duplicateChannels.put("proxy-a", mock(Channel.class));
        duplicateChannels.put(" proxy-a ", mock(Channel.class));
        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcTransport(duplicateChannels, new RecordingInvoker("{}")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate proxyId")
            .hasMessageContaining("proxy-a");
    }

    @Test
    public void grpcTransportRejectsMissingInvokerAndBlankProxyId() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));

        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcTransport(channels, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invoker is required");

        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            new RecordingInvoker("{}")
        );
        assertThatThrownBy(() -> transport.execute(proxyContext(), " ", "{}"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId is required");
    }

    private static ProxyContext proxyContext() {
        return ProxyContext.create()
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
    }

    private static class RecordingInvoker implements ProxyClientAdminPeerGrpcTransport.Invoker {
        private final String responseMessage;
        private Channel channel;
        private String requestMessage;

        private RecordingInvoker(String responseMessage) {
            this.responseMessage = responseMessage;
        }

        @Override
        public String execute(Channel channel, String requestMessage) {
            this.channel = channel;
            this.requestMessage = requestMessage;
            return this.responseMessage;
        }
    }
}
