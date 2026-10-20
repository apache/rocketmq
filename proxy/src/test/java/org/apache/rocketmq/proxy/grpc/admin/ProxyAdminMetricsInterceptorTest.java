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
import io.grpc.StatusRuntimeException;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminMetricsInterceptorTest {

    private static final String METHOD = "PrintThreadStackTrace";

    @Mock
    private ServerCall<byte[], byte[]> call;
    @Mock
    private ServerCallHandler<byte[], byte[]> next;
    @Mock
    private ServerCall.Listener<byte[]> innerListener;

    private ProxyAdminMetricsInterceptor interceptor;
    private ServerCall<byte[], byte[]> wrapped;
    private ServerCall.Listener<byte[]> outerListener;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        interceptor = new ProxyAdminMetricsInterceptor();
        MethodDescriptor<byte[], byte[]> descriptor = mock(MethodDescriptor.class);
        when(descriptor.getBareMethodName()).thenReturn(METHOD);
        when(call.getMethodDescriptor()).thenReturn(descriptor);
        when(next.startCall(any(), any())).thenReturn(innerListener);

        outerListener = interceptor.interceptCall(call, new Metadata(), next);
        ArgumentCaptor<ServerCall<byte[], byte[]>> captor = ArgumentCaptor.forClass(ServerCall.class);
        verify(next).startCall(captor.capture(), any());
        wrapped = captor.getValue();
    }

    @Test
    public void recordsTerminalStatusOnClose() {
        Metadata trailers = new Metadata();
        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            wrapped.close(Status.OK, trailers);
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(eq(METHOD), eq("OK"), anyLong()));
        }
        verify(call).close(Status.OK, trailers);
    }

    @Test
    public void recordsRejectedStatus() {
        Status denied = Status.PERMISSION_DENIED.withDescription("no acl mapping");
        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            wrapped.close(denied, new Metadata());
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq(METHOD), eq("PERMISSION_DENIED"), anyLong()));
        }
        verify(call).close(eq(denied), any(Metadata.class));
    }

    @Test
    public void cancelRecordedOnceEvenIfHandlerClosesLater() {
        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            outerListener.onCancel();
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq(METHOD), eq("CANCELLED"), anyLong()), times(1));

            wrapped.close(Status.CANCELLED, new Metadata());
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                anyString(), anyString(), anyLong()), times(1));
        }
    }

    @Test
    public void closeAfterNormalResponseNotCountedByCancel() {
        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            wrapped.close(Status.OK, new Metadata());
            outerListener.onCancel();
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq(METHOD), eq("OK"), anyLong()), times(1));
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq(METHOD), eq("CANCELLED"), anyLong()), never());
        }
    }

    @Test
    public void serviceExceptionRecordedAsInternalAndRethrown() {
        doThrow(new IllegalStateException("boom")).when(innerListener).onHalfClose();

        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            Assert.assertThrows(IllegalStateException.class, () -> outerListener.onHalfClose());
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq(METHOD), eq("INTERNAL"), anyLong()), times(1));
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                anyString(), anyString(), anyLong()), times(1));
        }
    }

    @Test
    public void serviceStatusExceptionRecordedWithItsOwnCode() {
        doThrow(Status.PERMISSION_DENIED.asRuntimeException()).when(innerListener).onHalfClose();

        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            Assert.assertThrows(StatusRuntimeException.class, () -> outerListener.onHalfClose());
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq(METHOD), eq("PERMISSION_DENIED"), anyLong()));
        }
    }

    @Test
    public void builtinMethodWithoutBareNameRecordsFullMethodName() {
        @SuppressWarnings("unchecked")
        ServerCall<byte[], byte[]> localCall = mock(ServerCall.class);
        @SuppressWarnings("unchecked")
        ServerCallHandler<byte[], byte[]> localNext = mock(ServerCallHandler.class);
        MethodDescriptor<byte[], byte[]> descriptor = mock(MethodDescriptor.class);
        when(descriptor.getBareMethodName()).thenReturn(null);
        when(descriptor.getFullMethodName()).thenReturn("grpc.health.v1.Health/Check");
        when(localCall.getMethodDescriptor()).thenReturn(descriptor);
        when(localNext.startCall(any(), any())).thenReturn(innerListener);

        new ProxyAdminMetricsInterceptor().interceptCall(localCall, new Metadata(), localNext);
        ArgumentCaptor<ServerCall<byte[], byte[]>> captor = ArgumentCaptor.forClass(ServerCall.class);
        verify(localNext).startCall(captor.capture(), any());

        try (MockedStatic<ProxyMetricsManager> metrics = mockStatic(ProxyMetricsManager.class)) {
            captor.getValue().close(Status.OK, new Metadata());
            metrics.verify(() -> ProxyMetricsManager.recordAdminCall(
                eq("grpc.health.v1.Health/Check"), eq("OK"), anyLong()));
        }
    }
}
