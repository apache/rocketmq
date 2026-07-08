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
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProxyClientAdminPeerGrpcTransportTest {

    @Test
    public void grpcTransportPropagatesProxyContextMetadataToPeerService() throws Exception {
        AtomicReference<Metadata> capturedHeaders = new AtomicReference<>();
        ProxyClientAdminContextFactory contextFactory = new ProxyClientAdminContextFactory(
            (context, headers, request) -> capturedHeaders.set(headers)
        );
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        String responseMessage = ProxyClientAdminPeerMessageCodec.getInstance().encodePageResponse(
            ProxyClientAdminPeerResponse.success("proxy-a", new ProxyClientPage(Collections.emptyList(), ""))
        );
        when(messageHandler.execute(any(), anyString())).thenReturn(responseMessage);
        Server server = NettyServerBuilder.forPort(0)
            .directExecutor()
            .addService(ServerInterceptors.intercept(
                new ProxyClientAdminPeerGrpcService(contextFactory, messageHandler),
                new ContextInterceptor()
            ))
            .build()
            .start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
            .usePlaintext()
            .directExecutor()
            .build();
        try {
            Map<String, Channel> channels = new LinkedHashMap<>();
            channels.put("proxy-a", channel);
            ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels);

            transport.execute(
                proxyContext()
                    .setSubject(User.of("admin"))
                    .setClientID("client-a")
                    .setLanguage("JAVA")
                    .setClientVersion("V5_0_0")
                    .setNamespace("namespace-a"),
                "proxy-a",
                "{\"operation\":\"LIST_CLIENTS\"}"
            );

            Metadata headers = capturedHeaders.get();
            assertThat(headers).isNotNull();
            assertThat(headers.get(GrpcConstants.AUTHORIZATION_AK)).isEqualTo("admin");
            assertThat(headers.get(GrpcConstants.REMOTE_ADDRESS)).isEqualTo("127.0.0.1:8080");
            assertThat(headers.get(GrpcConstants.LOCAL_ADDRESS)).isEqualTo("127.0.0.1:8081");
            assertThat(headers.get(GrpcConstants.CLIENT_ID)).isEqualTo("client-a");
            assertThat(headers.get(GrpcConstants.LANGUAGE)).isEqualTo("JAVA");
            assertThat(headers.get(GrpcConstants.CLIENT_VERSION)).isEqualTo("V5_0_0");
            assertThat(headers.get(GrpcConstants.NAMESPACE_ID)).isEqualTo("namespace-a");
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void grpcTransportMapsPeerServiceFailureWithDescriptionToEncodedPeerError() throws Exception {
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(contextFactory.create(any(Metadata.class), any())).thenReturn(ProxyContext.create());
        when(messageHandler.execute(any(), anyString())).thenThrow(new IllegalStateException("handler failed"));
        Server server = NettyServerBuilder.forPort(0)
            .directExecutor()
            .addService(new ProxyClientAdminPeerGrpcService(contextFactory, messageHandler))
            .build()
            .start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
            .usePlaintext()
            .directExecutor()
            .build();
        try {
            Map<String, Channel> channels = new LinkedHashMap<>();
            channels.put("proxy-a", channel);
            ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels);

            String responseMessage = transport.execute(
                proxyContext(),
                "proxy-a",
                "{\"operation\":\"LIST_CLIENTS\"}"
            );
            ProxyClientAdminPeerResponse<ProxyClientPage> response =
                ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getBody()).isNull();
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("handler failed");
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void grpcTransportMapsPeerServiceGrpcStatusToEncodedPeerError() throws Exception {
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(contextFactory.create(any(Metadata.class), any())).thenReturn(ProxyContext.create());
        when(messageHandler.execute(any(), anyString()))
            .thenThrow(Status.UNAVAILABLE.withDescription("peer unavailable").asRuntimeException());
        Server server = NettyServerBuilder.forPort(0)
            .directExecutor()
            .addService(new ProxyClientAdminPeerGrpcService(contextFactory, messageHandler))
            .build()
            .start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
            .usePlaintext()
            .directExecutor()
            .build();
        try {
            Map<String, Channel> channels = new LinkedHashMap<>();
            channels.put("proxy-a", channel);
            ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels);

            String responseMessage = transport.execute(
                proxyContext(),
                "proxy-a",
                "{\"operation\":\"LIST_CLIENTS\"}"
            );
            ProxyClientAdminPeerResponse<ProxyClientPage> response =
                ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getErrorCode()).isEqualTo(Code.PROXY_TIMEOUT.name());
            assertThat(response.getErrorMessage()).contains("peer unavailable");
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void grpcTransportListsStableProxyIdsAndInvokesTargetChannel() {
        Channel proxyBChannel = mock(Channel.class);
        Channel proxyAChannel = mock(Channel.class);
        String responseMessage = ProxyClientAdminPeerMessageCodec.getInstance().encodePageResponse(
            ProxyClientAdminPeerResponse.success("proxy-b", new ProxyClientPage(Collections.emptyList(), ""))
        );
        RecordingInvoker invoker = new RecordingInvoker(responseMessage);
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put(" proxy-b ", proxyBChannel);
        channels.put(" proxy-a ", proxyAChannel);
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        String actualResponseMessage =
            transport.execute(proxyContext(), " proxy-b ", "{\"operation\":\"LIST_CLIENTS\"}");

        assertThat(transport.listProxyIds()).containsExactly("proxy-a", "proxy-b");
        assertThat(actualResponseMessage).isEqualTo(responseMessage);
        assertThat(invoker.channel).isSameAs(proxyBChannel);
        assertThat(invoker.requestMessage).isEqualTo("{\"operation\":\"LIST_CLIENTS\"}");
        assertThat(invoker.metadata.get(GrpcConstants.REMOTE_ADDRESS)).isEqualTo("127.0.0.1:8080");
        assertThat(invoker.metadata.get(GrpcConstants.LOCAL_ADDRESS)).isEqualTo("127.0.0.1:8081");
    }

    @Test
    public void grpcTransportDoesNotForwardStaleKnownMetadataWhenContextOmitsIt() {
        Metadata.Key<String> customKey = Metadata.Key.of("x-custom", Metadata.ASCII_STRING_MARSHALLER);
        Metadata currentHeaders = new Metadata();
        currentHeaders.put(GrpcConstants.AUTHORIZATION_AK, "stale-admin");
        currentHeaders.put(GrpcConstants.REMOTE_ADDRESS, "stale-remote");
        currentHeaders.put(GrpcConstants.CLIENT_ID, "stale-client");
        currentHeaders.put(GrpcConstants.LANGUAGE, "stale-language");
        currentHeaders.put(customKey, "custom-value");
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        String responseMessage = ProxyClientAdminPeerMessageCodec.getInstance().encodePageResponse(
            ProxyClientAdminPeerResponse.success("proxy-a", new ProxyClientPage(Collections.emptyList(), ""))
        );
        RecordingInvoker invoker = new RecordingInvoker(responseMessage);
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        Context.current().withValue(GrpcConstants.METADATA, currentHeaders).run(() ->
            transport.execute(
                ProxyContext.create().setSubject(User.of("fresh-admin")),
                "proxy-a",
                "{\"operation\":\"LIST_CLIENTS\"}"
            )
        );

        assertThat(invoker.metadata.get(GrpcConstants.AUTHORIZATION_AK)).isEqualTo("fresh-admin");
        assertThat(invoker.metadata.get(GrpcConstants.REMOTE_ADDRESS)).isNull();
        assertThat(invoker.metadata.get(GrpcConstants.CLIENT_ID)).isNull();
        assertThat(invoker.metadata.get(GrpcConstants.LANGUAGE)).isNull();
        assertThat(invoker.metadata.get(customKey)).isEqualTo("custom-value");
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
            (channel, requestMessage, metadata) -> {
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
    public void grpcTransportRejectsBlankRequestMessageBeforeCallingPeer() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        RecordingInvoker invoker = new RecordingInvoker(ProxyClientAdminPeerMessageCodec.getInstance()
            .encodePageResponse(ProxyClientAdminPeerResponse.success(
                "proxy-a",
                new ProxyClientPage(Collections.emptyList(), "")
            )));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        String responseMessage = transport.execute(proxyContext(), " proxy-a ", " ");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("peer request message is required");
        assertThat(invoker.channel).isNull();
    }

    @Test
    public void grpcTransportRejectsOverlongRequestMessageBeforeCallingPeer() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        RecordingInvoker invoker = new RecordingInvoker(ProxyClientAdminPeerMessageCodec.getInstance()
            .encodePageResponse(ProxyClientAdminPeerResponse.success(
                "proxy-a",
                new ProxyClientPage(Collections.emptyList(), "")
            )));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        String responseMessage = transport.execute(
            proxyContext(),
            "proxy-a",
            org.apache.commons.lang3.StringUtils.repeat("a", 1024 * 1024 + 1)
        );
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("peer request message length exceeds");
        assertThat(invoker.channel).isNull();
    }

    @Test
    public void grpcTransportRejectsMalformedRequestMessageBeforeCallingPeer() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        RecordingInvoker invoker = new RecordingInvoker(ProxyClientAdminPeerMessageCodec.getInstance()
            .encodePageResponse(ProxyClientAdminPeerResponse.success(
                "proxy-a",
                new ProxyClientPage(Collections.emptyList(), "")
            )));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("Invalid peer request message");
        assertThat(invoker.channel).isNull();
    }

    @Test
    public void grpcTransportMapsOverlongPeerResponseToEncodedPeerError() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            new RecordingInvoker(org.apache.commons.lang3.StringUtils.repeat("a", 1024 * 1024 + 1))
        );

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
        assertThat(response.getErrorMessage()).contains("peer response message length exceeds");
    }

    @Test
    public void grpcTransportMapsMalformedPeerResponseToEncodedPeerError() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            new RecordingInvoker("{")
        );

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
        assertThat(response.getErrorMessage()).contains("Invalid peer response message");
    }

    @Test
    public void grpcTransportMapsSuccessfulPeerResponseWithoutExpectedBodyToEncodedPeerError() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            new RecordingInvoker("{\"proxyId\":\"proxy-a\",\"success\":true}")
        );

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
        assertThat(response.getErrorMessage()).contains("peer page response body is required");
    }

    @Test
    public void grpcTransportSendsNormalizedRequestMessageToPeer() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        RecordingInvoker invoker = new RecordingInvoker(ProxyClientAdminPeerMessageCodec.getInstance()
            .encodePageResponse(ProxyClientAdminPeerResponse.success(
                "proxy-a",
                new ProxyClientPage(Collections.emptyList(), "")
            )));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(channels, invoker);

        transport.execute(proxyContext(), " proxy-a ", " {\"operation\":\"LIST_CLIENTS\"} ");

        assertThat(invoker.requestMessage).isEqualTo("{\"operation\":\"LIST_CLIENTS\"}");
    }

    @Test
    public void grpcTransportMapsGrpcStatusFailuresToEncodedPeerError() {
        assertGrpcStatusMapsToPeerError(Status.PERMISSION_DENIED.withDescription("denied"),
            Code.UNAUTHORIZED, "denied");
        assertGrpcStatusMapsToPeerError(Status.UNAUTHENTICATED.withDescription("missing credentials"),
            Code.UNAUTHORIZED, "missing credentials");
        assertGrpcStatusMapsToPeerError(Status.INVALID_ARGUMENT.withDescription("bad request"),
            Code.BAD_REQUEST, "bad request");
        assertGrpcStatusMapsToPeerError(Status.NOT_FOUND.withDescription("missing proxy"),
            Code.NOT_FOUND, "missing proxy");
    }

    @Test
    public void grpcTransportMapsUnavailablePeerToProxyTimeout() {
        assertGrpcStatusMapsToPeerError(Status.UNAVAILABLE.withDescription("peer unavailable"),
            Code.PROXY_TIMEOUT, "peer unavailable");
    }

    @Test
    public void grpcTransportMapsGrpcCheckedStatusFailuresToEncodedPeerError() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            (channel, requestMessage, metadata) -> {
                throwUnchecked(Status.UNAVAILABLE.withDescription("peer unavailable").asException());
                return null;
            }
        );

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(Code.PROXY_TIMEOUT.name());
        assertThat(response.getErrorMessage()).contains("peer unavailable");
    }

    @Test
    public void grpcTransportMapsWrappedGrpcStatusFailuresToEncodedPeerError() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            (channel, requestMessage, metadata) -> {
                throw new CompletionException(
                    Status.DEADLINE_EXCEEDED.withDescription("peer deadline exceeded").asRuntimeException()
                );
            }
        );

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(Code.PROXY_TIMEOUT.name());
        assertThat(response.getErrorMessage()).contains("peer deadline exceeded");
    }

    @Test
    public void grpcTransportRestoresInterruptWhenInvokerIsInterrupted() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            (channel, requestMessage, metadata) -> {
                throwUnchecked(new InterruptedException("peer call interrupted"));
                return null;
            }
        );

        try {
            String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
            ProxyClientAdminPeerResponse<ProxyClientPage> response =
                ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("peer call interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void grpcTransportRestoresInterruptWhenInvokerFailureWrapsInterruptedException() {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            (channel, requestMessage, metadata) -> {
                throw new CompletionException(new InterruptedException("wrapped peer call interrupted"));
            }
        );

        try {
            String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
            ProxyClientAdminPeerResponse<ProxyClientPage> response =
                ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("wrapped peer call interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
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
    public void grpcTransportRejectsOverlongProxyIds() {
        String proxyId = StringUtils.repeat("p", 256);

        assertThatThrownBy(() -> {
            Map<String, Channel> channels = new LinkedHashMap<>();
            channels.put(proxyId, mock(Channel.class));
            new ProxyClientAdminPeerGrpcTransport(channels, new RecordingInvoker("{}"));
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");
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

    private static void assertGrpcStatusMapsToPeerError(Status grpcStatus, Code expectedCode,
        String expectedMessage) {
        Map<String, Channel> channels = new LinkedHashMap<>();
        channels.put("proxy-a", mock(Channel.class));
        ProxyClientAdminPeerGrpcTransport transport = new ProxyClientAdminPeerGrpcTransport(
            channels,
            (channel, requestMessage, metadata) -> {
                throw new StatusRuntimeException(grpcStatus);
            }
        );

        String responseMessage = transport.execute(proxyContext(), "proxy-a", "{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getErrorCode()).isEqualTo(expectedCode.name());
        assertThat(response.getErrorMessage()).contains(expectedMessage);
    }

    private static void throwUnchecked(StatusException statusException) {
        ProxyClientAdminPeerGrpcTransportTest.<RuntimeException>throwAny(statusException);
    }

    private static void throwUnchecked(InterruptedException interruptedException) {
        ProxyClientAdminPeerGrpcTransportTest.<RuntimeException>throwAny(interruptedException);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwAny(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static class RecordingInvoker implements ProxyClientAdminPeerGrpcTransport.Invoker {
        private final String responseMessage;
        private Channel channel;
        private String requestMessage;
        private Metadata metadata;

        private RecordingInvoker(String responseMessage) {
            this.responseMessage = responseMessage;
        }

        @Override
        public String execute(Channel channel, String requestMessage, Metadata metadata) {
            this.channel = channel;
            this.requestMessage = requestMessage;
            this.metadata = metadata;
            return this.responseMessage;
        }
    }
}
