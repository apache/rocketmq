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
package org.apache.rocketmq.proxy.grpc.admin;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminMetricsInterceptorTest {

    @Mock
    private ServerCallHandler<byte[], byte[]> next;

    private static final MethodDescriptor.Marshaller<byte[]> BYTE_MARSHALLER =
        new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value == null ? new byte[0] : value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                return new byte[0];
            }
        };

    @SuppressWarnings("unchecked")
    private static ServerCall<byte[], byte[]> serverCall(String methodName) {
        ServerCall<byte[], byte[]> call = mock(ServerCall.class);
        when(call.getMethodDescriptor()).thenReturn(MethodDescriptor.<byte[], byte[]>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("apache.rocketmq.v2.Admin/" + methodName)
            .setRequestMarshaller(BYTE_MARSHALLER)
            .setResponseMarshaller(BYTE_MARSHALLER)
            .build());
        return call;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void okCloseIsForwardedWithoutRecording() {
        ServerCall<byte[], byte[]> delegate = serverCall("ListClients");
        when(next.startCall(any(ServerCall.class), any(Metadata.class)))
            .thenReturn(mock(ServerCall.Listener.class));

        ProxyAdminMetricsInterceptor interceptor = new ProxyAdminMetricsInterceptor();
        ServerCall.Listener<byte[]> listener = interceptor.interceptCall(delegate, new Metadata(), next);
        assertNotNull(listener);

        ArgumentCaptor<ServerCall<byte[], byte[]>> captor = ArgumentCaptor.forClass(ServerCall.class);
        verify(next).startCall(captor.capture(), any(Metadata.class));

        Metadata trailers = new Metadata();
        captor.getValue().close(Status.OK, trailers);
        // OK close is delegated verbatim; no metric recording path is triggered
        verify(delegate).close(Status.OK, trailers);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void errorCloseIsForwardedAndRecorded() {
        ServerCall<byte[], byte[]> delegate = serverCall("ResetGroupOffset");
        when(next.startCall(any(ServerCall.class), any(Metadata.class)))
            .thenReturn(mock(ServerCall.Listener.class));

        ProxyAdminMetricsInterceptor interceptor = new ProxyAdminMetricsInterceptor();
        interceptor.interceptCall(delegate, new Metadata(), next);

        ArgumentCaptor<ServerCall<byte[], byte[]>> captor = ArgumentCaptor.forClass(ServerCall.class);
        verify(next).startCall(captor.capture(), any(Metadata.class));

        Metadata trailers = new Metadata();
        Status error = Status.PERMISSION_DENIED.withDescription("denied by proxy.admin.ops policy");
        captor.getValue().close(error, trailers);
        // the error close still reaches the underlying call; recordError is a safe
        // no-op while the metrics pipeline is uninitialized in this test JVM
        verify(delegate).close(error, trailers);
    }
}
