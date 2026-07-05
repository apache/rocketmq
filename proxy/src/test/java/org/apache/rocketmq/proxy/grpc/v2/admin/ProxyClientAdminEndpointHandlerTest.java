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
import apache.rocketmq.v2.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminEndpointHandlerTest {

    @Test
    public void listClientsDelegatesToActivityAndWritesResponse() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler(activity);
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);
        ProxyContext ctx = ProxyContext.create();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder().build();
        ProxyClientAdminPageView pageView = new ProxyClientAdminPageView(Collections.emptyList(), "next-client");
        when(activity.listClientViews(ctx, request)).thenReturn(okResult(pageView));

        handler.listClients(ctx, request, observer, TestAdminResponse::new);

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(responseCaptor.getValue().getBody()).isSameAs(pageView);
    }

    @Test
    public void listClientsWithoutActivityWritesInternalServerError() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.listClients(
            ProxyContext.create(),
            ProxyClientAdminListClientsRequest.newBuilder().build(),
            observer,
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(responseCaptor.getValue().getStatus().getMessage())
            .contains("proxyClientAdminActivity is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void describeClientDelegatesToActivityAndWritesResponse() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler(activity);
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);
        ProxyContext ctx = ProxyContext.create();
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .build();
        ProxyClientAdminClientView clientView = clientView("client-a");
        when(activity.describeClientView(ctx, request)).thenReturn(okResult(clientView));

        handler.describeClient(ctx, request, observer, TestAdminResponse::new);

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(responseCaptor.getValue().getBody()).isSameAs(clientView);
    }

    @Test
    public void listClientsByGroupDelegatesToActivityAndWritesResponse() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler(activity);
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);
        ProxyContext ctx = ProxyContext.create();
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup("group-a")
            .build();
        ProxyClientAdminPageView pageView = new ProxyClientAdminPageView(Collections.emptyList(), "");
        when(activity.listClientViewsByGroup(ctx, request)).thenReturn(okResult(pageView));

        handler.listClientsByGroup(ctx, request, observer, TestAdminResponse::new);

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(responseCaptor.getValue().getBody()).isSameAs(pageView);
    }

    @Test
    public void listClientsByTopicDelegatesToActivityAndWritesResponse() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler(activity);
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);
        ProxyContext ctx = ProxyContext.create();
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic("topic-a")
            .build();
        ProxyClientAdminPageView pageView = new ProxyClientAdminPageView(Collections.emptyList(), "");
        when(activity.listClientViewsByTopic(ctx, request)).thenReturn(okResult(pageView));

        handler.listClientsByTopic(ctx, request, observer, TestAdminResponse::new);

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(responseCaptor.getValue().getBody()).isSameAs(pageView);
    }

    @Test
    public void handleWritesOkResultAndCompletesObserver() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                "client-a"
            ),
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(responseCaptor.getValue().getBody()).isEqualTo("client-a");
    }

    @Test
    public void handleMapsOkResultWithoutBodyToStatusResponse() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                null
            ),
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("result body is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleWritesErrorResultWithoutBody() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.NOT_FOUND, "missing client"),
                null
            ),
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("missing client");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleDropsErrorResultBody() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.NOT_FOUND, "missing client"),
                "stale-body"
            ),
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("missing client");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleMapsThrownActionToStatusResponse() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> {
                throw new IllegalArgumentException("request is required");
            },
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("request is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleRejectsMissingResponseObserverBeforeExecutingAction() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        AtomicBoolean actionInvoked = new AtomicBoolean(false);

        assertThatThrownBy(() -> handler.handle(
            null,
            () -> {
                actionInvoked.set(true);
                return okResult("client-a");
            },
            TestAdminResponse::new
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("responseObserver is required");
        assertThat(actionInvoked).isFalse();
    }

    @Test
    public void handleMapsMissingResponseFactoryToGrpcInternalErrorBeforeExecutingAction() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);
        AtomicBoolean actionInvoked = new AtomicBoolean(false);

        handler.handle(
            observer,
            () -> {
                actionInvoked.set(true);
                return okResult("client-a");
            },
            null
        );

        ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue()).isInstanceOf(io.grpc.StatusRuntimeException.class);
        io.grpc.StatusRuntimeException statusRuntimeException =
            (io.grpc.StatusRuntimeException) errorCaptor.getValue();
        assertThat(statusRuntimeException.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
        assertThat(statusRuntimeException.getStatus().getDescription()).contains("responseFactory is required");
        assertThat(actionInvoked).isFalse();
        verify(observer, never()).onNext(any());
        verify(observer, never()).onCompleted();
    }

    @Test
    public void handleMapsResponseFactoryFailureToStatusResponse() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> okResult("client-a"),
            (status, body) -> {
                if (status.getCode() == Code.OK) {
                    throw new IllegalStateException("response conversion failed");
                }
                return new TestAdminResponse(status, body);
            }
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("response conversion failed");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleMapsNullResponseFactoryResultToStatusResponse() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> okResult("client-a"),
            (status, body) -> {
                if (status.getCode() == Code.OK) {
                    return null;
                }
                return new TestAdminResponse(status, body);
            }
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("response is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleMapsRepeatedResponseFactoryFailureToGrpcInternalError() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> okResult("client-a"),
            (status, body) -> {
                throw new IllegalStateException("response conversion failed");
            }
        );

        ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue()).isInstanceOf(io.grpc.StatusRuntimeException.class);
        io.grpc.StatusRuntimeException statusRuntimeException =
            (io.grpc.StatusRuntimeException) errorCaptor.getValue();
        assertThat(statusRuntimeException.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
        assertThat(statusRuntimeException.getStatus().getDescription()).contains("response conversion failed");
        verify(observer, never()).onNext(any());
        verify(observer, never()).onCompleted();
    }

    private static <T> ProxyClientAdminResult<T> okResult(T body) {
        return new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
            body
        );
    }

    private static ProxyClientAdminClientView clientView(String clientId) {
        return new ProxyClientAdminClientView(
            clientId,
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            "",
            "",
            "",
            "",
            0,
            0
        );
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
