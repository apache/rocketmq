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
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.Status;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyClientAdminEndpointExecutorTest {
    private final Metadata headers = new Metadata();
    private final QueryRouteRequest protoRequest = QueryRouteRequest.getDefaultInstance();
    private final ProxyContext ctx = ProxyContext.create();

    @Mock
    private ProxyClientAdminContextFactory contextFactory;
    @Mock
    private ProxyClientAdminEndpointHandler endpointHandler;
    @Mock
    private StreamObserver<TestAdminResponse> responseObserver;

    private ProxyClientAdminEndpointExecutor executor;

    @Before
    public void setUp() {
        this.executor = new ProxyClientAdminEndpointExecutor(contextFactory, endpointHandler);
    }

    @Test
    public void listClientsBuildsContextAndDelegatesToEndpointHandler() {
        ProxyClientAdminListClientsRequest internalRequest =
            ProxyClientAdminListClientsRequest.newBuilder().build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(headers, protoRequest)).thenReturn(ctx);

        executor.listClients(
            headers,
            protoRequest,
            ignored -> internalRequest,
            responseObserver,
            responseFactory
        );

        verify(endpointHandler).listClients(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void listClientsUsesMetadataFromCurrentGrpcContext() {
        Metadata currentHeaders = new Metadata();
        ProxyClientAdminListClientsRequest internalRequest =
            ProxyClientAdminListClientsRequest.newBuilder().build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(currentHeaders, protoRequest)).thenReturn(ctx);

        Context.current().withValue(GrpcConstants.METADATA, currentHeaders).run(() ->
            executor.listClients(
                protoRequest,
                ignored -> internalRequest,
                responseObserver,
                responseFactory
            )
        );

        verify(endpointHandler).listClients(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void listClientsUsesEmptyMetadataWhenCurrentGrpcContextHasNoMetadata() {
        ProxyClientAdminListClientsRequest internalRequest =
            ProxyClientAdminListClientsRequest.newBuilder().build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;

        executor.listClients(
            protoRequest,
            ignored -> internalRequest,
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<Metadata> headersCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(contextFactory).create(headersCaptor.capture(), same(protoRequest));
        assertThat(headersCaptor.getValue()).isNotNull();
    }

    @Test
    public void listClientsUsesEmptyMetadataWhenExplicitHeadersAreMissing() {
        ProxyClientAdminListClientsRequest internalRequest =
            ProxyClientAdminListClientsRequest.newBuilder().build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(any(Metadata.class), same(protoRequest))).thenReturn(ctx);

        executor.listClients(
            null,
            protoRequest,
            ignored -> internalRequest,
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<Metadata> headersCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(contextFactory).create(headersCaptor.capture(), same(protoRequest));
        assertThat(headersCaptor.getValue()).isNotNull();
        verify(endpointHandler).listClients(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void describeClientBuildsContextAndDelegatesToEndpointHandler() {
        ProxyClientAdminDescribeClientRequest internalRequest =
            ProxyClientAdminDescribeClientRequest.newBuilder().setClientId("client-a").build();
        BiFunction<Status, ProxyClientAdminClientView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(headers, protoRequest)).thenReturn(ctx);

        executor.describeClient(
            headers,
            protoRequest,
            ignored -> internalRequest,
            responseObserver,
            responseFactory
        );

        verify(endpointHandler).describeClient(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void describeClientUsesMetadataFromCurrentGrpcContext() {
        Metadata currentHeaders = new Metadata();
        ProxyClientAdminDescribeClientRequest internalRequest =
            ProxyClientAdminDescribeClientRequest.newBuilder().setClientId("client-a").build();
        BiFunction<Status, ProxyClientAdminClientView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(currentHeaders, protoRequest)).thenReturn(ctx);

        Context.current().withValue(GrpcConstants.METADATA, currentHeaders).run(() ->
            executor.describeClient(
                protoRequest,
                ignored -> internalRequest,
                responseObserver,
                responseFactory
            )
        );

        verify(endpointHandler).describeClient(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void listClientsByGroupBuildsContextAndDelegatesToEndpointHandler() {
        ProxyClientAdminListClientsByGroupRequest internalRequest =
            ProxyClientAdminListClientsByGroupRequest.newBuilder().setGroup("group-a").build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(headers, protoRequest)).thenReturn(ctx);

        executor.listClientsByGroup(
            headers,
            protoRequest,
            ignored -> internalRequest,
            responseObserver,
            responseFactory
        );

        verify(endpointHandler).listClientsByGroup(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void listClientsByGroupUsesMetadataFromCurrentGrpcContext() {
        Metadata currentHeaders = new Metadata();
        ProxyClientAdminListClientsByGroupRequest internalRequest =
            ProxyClientAdminListClientsByGroupRequest.newBuilder().setGroup("group-a").build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(currentHeaders, protoRequest)).thenReturn(ctx);

        Context.current().withValue(GrpcConstants.METADATA, currentHeaders).run(() ->
            executor.listClientsByGroup(
                protoRequest,
                ignored -> internalRequest,
                responseObserver,
                responseFactory
            )
        );

        verify(endpointHandler).listClientsByGroup(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void listClientsByTopicBuildsContextAndDelegatesToEndpointHandler() {
        ProxyClientAdminListClientsByTopicRequest internalRequest =
            ProxyClientAdminListClientsByTopicRequest.newBuilder().setTopic("topic-a").build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(headers, protoRequest)).thenReturn(ctx);

        executor.listClientsByTopic(
            headers,
            protoRequest,
            ignored -> internalRequest,
            responseObserver,
            responseFactory
        );

        verify(endpointHandler).listClientsByTopic(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void listClientsByTopicUsesMetadataFromCurrentGrpcContext() {
        Metadata currentHeaders = new Metadata();
        ProxyClientAdminListClientsByTopicRequest internalRequest =
            ProxyClientAdminListClientsByTopicRequest.newBuilder().setTopic("topic-a").build();
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(currentHeaders, protoRequest)).thenReturn(ctx);

        Context.current().withValue(GrpcConstants.METADATA, currentHeaders).run(() ->
            executor.listClientsByTopic(
                protoRequest,
                ignored -> internalRequest,
                responseObserver,
                responseFactory
            )
        );

        verify(endpointHandler).listClientsByTopic(
            same(ctx),
            same(internalRequest),
            same(responseObserver),
            same(responseFactory)
        );
    }

    @Test
    public void mapsRequestAdapterFailureToStatusResponseBeforeCreatingContext() {
        ProxyClientAdminEndpointExecutor executor =
            new ProxyClientAdminEndpointExecutor(contextFactory, new ProxyClientAdminEndpointHandler());
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;

        executor.listClients(
            headers,
            protoRequest,
            ignored -> {
                throw new IllegalArgumentException("page token is invalid");
            },
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("page token is invalid");
        assertThat(responseCaptor.getValue().getBody()).isNull();
        verify(contextFactory, never()).create(any(), any());
        verify(endpointHandler, never()).listClients(any(), any(), any(), any());
    }

    @Test
    public void mapsContextFactoryFailureToStatusResponseAfterAdaptingRequest() {
        ProxyClientAdminEndpointExecutor executor =
            new ProxyClientAdminEndpointExecutor(contextFactory, new ProxyClientAdminEndpointHandler());
        AtomicBoolean requestAdapterInvoked = new AtomicBoolean(false);
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;
        when(contextFactory.create(headers, protoRequest))
            .thenThrow(new IllegalArgumentException("request headers are invalid"));

        executor.listClients(
            headers,
            protoRequest,
            ignored -> {
                requestAdapterInvoked.set(true);
                return ProxyClientAdminListClientsRequest.newBuilder().build();
            },
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("request headers are invalid");
        assertThat(responseCaptor.getValue().getBody()).isNull();
        assertThat(requestAdapterInvoked).isTrue();
        verify(endpointHandler, never()).listClients(any(), any(), any(), any());
    }

    @Test
    public void listClientsMapsMissingRequestAdapterToStatusResponseBeforeCreatingContext() {
        ProxyClientAdminEndpointExecutor executor =
            new ProxyClientAdminEndpointExecutor(contextFactory, new ProxyClientAdminEndpointHandler());
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;

        executor.listClients(
            headers,
            protoRequest,
            null,
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("requestAdapter is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
        verify(contextFactory, never()).create(any(), any());
    }

    @Test
    public void listClientsMapsMissingProtoRequestToStatusResponseBeforeCreatingContext() {
        ProxyClientAdminEndpointExecutor executor =
            new ProxyClientAdminEndpointExecutor(contextFactory, new ProxyClientAdminEndpointHandler());
        AtomicBoolean requestAdapterInvoked = new AtomicBoolean(false);
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;

        executor.listClients(
            headers,
            null,
            ignored -> {
                requestAdapterInvoked.set(true);
                return ProxyClientAdminListClientsRequest.newBuilder().build();
            },
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("protoRequest is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
        assertThat(requestAdapterInvoked).isFalse();
        verify(contextFactory, never()).create(any(), any());
    }

    @Test
    public void mapsNullAdaptedRequestToBadRequest() {
        ProxyClientAdminEndpointExecutor executor =
            new ProxyClientAdminEndpointExecutor(contextFactory, new ProxyClientAdminEndpointHandler());
        BiFunction<Status, ProxyClientAdminPageView, TestAdminResponse> responseFactory = TestAdminResponse::new;

        executor.listClients(
            headers,
            protoRequest,
            ignored -> null,
            responseObserver,
            responseFactory
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage())
            .contains("requestAdapter result is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
        verify(contextFactory, never()).create(any(), any());
    }

    @Test
    public void mapsMissingResponseFactoryToGrpcInternalErrorBeforeCreatingContext() {
        ProxyClientAdminEndpointExecutor executor =
            new ProxyClientAdminEndpointExecutor(contextFactory, new ProxyClientAdminEndpointHandler());

        executor.listClients(
            headers,
            protoRequest,
            ignored -> ProxyClientAdminListClientsRequest.newBuilder().build(),
            responseObserver,
            null
        );

        ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(responseObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue()).isInstanceOf(io.grpc.StatusRuntimeException.class);
        io.grpc.StatusRuntimeException statusRuntimeException =
            (io.grpc.StatusRuntimeException) errorCaptor.getValue();
        assertThat(statusRuntimeException.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
        assertThat(statusRuntimeException.getStatus().getDescription()).contains("responseFactory is required");
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();
        verify(contextFactory, never()).create(any(), any());
        verify(endpointHandler, never()).listClients(any(), any(), any(), any());
    }

    @Test
    public void constructorRejectsMissingDependencies() {
        assertThatThrownBy(() -> new ProxyClientAdminEndpointExecutor(null, endpointHandler))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("contextFactory is required");
        assertThatThrownBy(() -> new ProxyClientAdminEndpointExecutor(contextFactory, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("endpointHandler is required");
    }

    private static class TestAdminResponse {
        private final Status status;
        private final Object body;

        private TestAdminResponse(Status status, Object body) {
            this.status = status;
            this.body = body;
        }

        private Status getStatus() {
            return status;
        }

        private Object getBody() {
            return body;
        }
    }
}
