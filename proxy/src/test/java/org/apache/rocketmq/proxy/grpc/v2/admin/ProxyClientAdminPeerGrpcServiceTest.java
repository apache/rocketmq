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

import com.google.protobuf.StringValue;
import io.grpc.Metadata;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Method;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminPeerGrpcServiceTest {

    @Test
    public void bindServiceExposesInternalUnaryPeerExecuteMethod() {
        ProxyClientAdminPeerGrpcService service = newService(
            mock(ProxyClientAdminContextFactory.class),
            mock(ProxyClientAdminPeerMessageHandler.class)
        );

        ServerServiceDefinition definition = service.bindService();

        assertThat(definition.getServiceDescriptor().getName())
            .isEqualTo(ProxyClientAdminPeerGrpcService.SERVICE_NAME);
        assertThat(definition.getMethods())
            .extracting(method -> method.getMethodDescriptor().getFullMethodName())
            .containsExactly(ProxyClientAdminPeerGrpcService.EXECUTE_METHOD.getFullMethodName());
        ServerMethodDefinition<?, ?> method = definition.getMethods().iterator().next();
        assertThat(method.getMethodDescriptor().getType())
            .isEqualTo(ProxyClientAdminPeerGrpcService.EXECUTE_METHOD.getType());
        assertThat(method.getMethodDescriptor().getRequestMarshaller())
            .isEqualTo(ProxyClientAdminPeerGrpcService.EXECUTE_METHOD.getRequestMarshaller());
        assertThat(method.getMethodDescriptor().getResponseMarshaller())
            .isEqualTo(ProxyClientAdminPeerGrpcService.EXECUTE_METHOD.getResponseMarshaller());
    }

    @Test
    public void executeCreatesProxyContextAndDelegatesToMessageHandler() {
        Metadata headers = new Metadata();
        ProxyContext ctx = ProxyContext.create().setRemoteAddress("127.0.0.1:8080");
        StringValue request = StringValue.of("{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(contextFactory.create(headers, request)).thenReturn(ctx);
        when(messageHandler.execute(ctx, request.getValue())).thenReturn("{\"success\":false}");
        ProxyClientAdminPeerGrpcService service = newService(contextFactory, messageHandler);

        StringValue response = service.execute(headers, request);

        assertThat(response.getValue()).isEqualTo("{\"success\":false}");
        verify(contextFactory).create(same(headers), same(request));
        verify(messageHandler).execute(same(ctx), same(request.getValue()));
    }

    @Test
    public void executeUsesEmptyMetadataWhenHeadersAreMissing() {
        ProxyContext ctx = ProxyContext.create();
        StringValue request = StringValue.of("{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(contextFactory.create(org.mockito.ArgumentMatchers.any(Metadata.class), same(request))).thenReturn(ctx);
        when(messageHandler.execute(ctx, request.getValue())).thenReturn("{\"success\":true}");
        ProxyClientAdminPeerGrpcService service = newService(contextFactory, messageHandler);

        StringValue response = service.execute(null, request);

        assertThat(response.getValue()).isEqualTo("{\"success\":true}");
    }

    @Test
    public void executeRejectsMissingRequestAndBlankHandlerResponse() {
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        ProxyClientAdminPeerGrpcService service = newService(contextFactory, messageHandler);

        assertThatThrownBy(() -> service.execute(new Metadata(), null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("request is required");

        ProxyContext ctx = ProxyContext.create();
        StringValue request = StringValue.of("{\"operation\":\"LIST_CLIENTS\"}");
        when(contextFactory.create(org.mockito.ArgumentMatchers.any(Metadata.class), same(request))).thenReturn(ctx);
        when(messageHandler.execute(ctx, request.getValue())).thenReturn(" ");

        assertThatThrownBy(() -> service.execute(new Metadata(), request))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("peer response message is required");
    }

    @Test
    public void asyncExecuteRestoresInterruptWhenHandlerIsInterrupted() throws Exception {
        ProxyContext ctx = ProxyContext.create();
        StringValue request = StringValue.of("{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(contextFactory.create(org.mockito.ArgumentMatchers.any(Metadata.class), same(request))).thenReturn(ctx);
        when(messageHandler.execute(same(ctx), anyString())).thenAnswer(invocation -> {
            throwUnchecked(new InterruptedException("peer service interrupted"));
            return null;
        });
        ProxyClientAdminPeerGrpcService service = newService(contextFactory, messageHandler);
        CapturingStreamObserver responseObserver = new CapturingStreamObserver();

        try {
            invokeAsyncExecute(service, request, responseObserver);

            assertThat(responseObserver.error).isInstanceOf(StatusRuntimeException.class);
            StatusRuntimeException error = (StatusRuntimeException) responseObserver.error;
            assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
            assertThat(error.getStatus().getDescription()).contains("peer service interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void asyncExecuteRestoresInterruptWhenHandlerFailureWrapsInterruptedException() throws Exception {
        ProxyContext ctx = ProxyContext.create();
        StringValue request = StringValue.of("{\"operation\":\"LIST_CLIENTS\"}");
        ProxyClientAdminContextFactory contextFactory = mock(ProxyClientAdminContextFactory.class);
        ProxyClientAdminPeerMessageHandler messageHandler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(contextFactory.create(org.mockito.ArgumentMatchers.any(Metadata.class), same(request))).thenReturn(ctx);
        when(messageHandler.execute(same(ctx), anyString())).thenThrow(
            new CompletionException(new InterruptedException("wrapped peer service interrupted"))
        );
        ProxyClientAdminPeerGrpcService service = newService(contextFactory, messageHandler);
        CapturingStreamObserver responseObserver = new CapturingStreamObserver();

        try {
            invokeAsyncExecute(service, request, responseObserver);

            assertThat(responseObserver.error).isInstanceOf(StatusRuntimeException.class);
            StatusRuntimeException error = (StatusRuntimeException) responseObserver.error;
            assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
            assertThat(error.getStatus().getDescription()).contains("wrapped peer service interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void constructorRejectsMissingDependencies() {
        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcService(null,
            mock(ProxyClientAdminPeerMessageHandler.class)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("contextFactory is required");
        assertThatThrownBy(() -> new ProxyClientAdminPeerGrpcService(
            mock(ProxyClientAdminContextFactory.class),
            null
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("messageHandler is required");
    }

    private static ProxyClientAdminPeerGrpcService newService(ProxyClientAdminContextFactory contextFactory,
        ProxyClientAdminPeerMessageHandler messageHandler) {
        return new ProxyClientAdminPeerGrpcService(contextFactory, messageHandler);
    }

    private static void invokeAsyncExecute(ProxyClientAdminPeerGrpcService service, StringValue request,
        StreamObserver<StringValue> responseObserver) throws Exception {
        Method method = ProxyClientAdminPeerGrpcService.class.getDeclaredMethod(
            "execute",
            StringValue.class,
            StreamObserver.class
        );
        method.setAccessible(true);
        method.invoke(service, request, responseObserver);
    }

    private static void throwUnchecked(InterruptedException interruptedException) {
        ProxyClientAdminPeerGrpcServiceTest.<RuntimeException>throwAny(interruptedException);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwAny(Throwable throwable) throws T {
        throw (T) throwable;
    }

    private static class CapturingStreamObserver implements StreamObserver<StringValue> {
        private Throwable error;

        @Override
        public void onNext(StringValue value) {
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
        }
    }
}
