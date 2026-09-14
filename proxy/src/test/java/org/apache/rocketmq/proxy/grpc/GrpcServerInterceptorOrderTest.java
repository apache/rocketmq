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

package org.apache.rocketmq.proxy.grpc;

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.List;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.grpc.constant.AttributeKeys;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The admin server authenticates in an interceptor, and authentication reads the channel id from the
 * request metadata. Since authentication results are cached per channel id, it must never run on the
 * client-supplied value: a second connection could otherwise present the same id and reuse the first
 * connection's successful authentication. These tests pin the two properties that together prevent
 * that — the ordering of the pipeline, and the header normalization it performs.
 */
public class GrpcServerInterceptorOrderTest {

    /**
     * gRPC invokes server interceptors in reverse registration order, so an interceptor that must
     * run after {@link HeaderInterceptor} has to be registered before it.
     */
    @Test
    public void postHeaderInterceptorsAreOrderedAfterHeaderInterceptorTest() {
        ServerInterceptor authInterceptor = mock(ServerInterceptor.class);
        @SuppressWarnings("unchecked")
        ServerBuilder<?> serverBuilder = mock(ServerBuilder.class);

        GrpcServerBuilder.configureInterceptors(serverBuilder, authInterceptor);

        ArgumentCaptor<ServerInterceptor> captor = ArgumentCaptor.forClass(ServerInterceptor.class);
        verify(serverBuilder, atLeastOnce()).intercept(captor.capture());
        List<ServerInterceptor> registrationOrder = captor.getAllValues();

        int authIndex = registrationOrder.indexOf(authInterceptor);
        int headerIndex = -1;
        for (int i = 0; i < registrationOrder.size(); i++) {
            if (registrationOrder.get(i) instanceof HeaderInterceptor) {
                headerIndex = i;
            }
        }

        assertSame(authInterceptor, registrationOrder.get(0));
        assertTrue("HeaderInterceptor must be registered", headerIndex >= 0);
        // registered earlier means executed later, so authentication observes normalized headers
        assertTrue("authentication must be registered before HeaderInterceptor so that it runs after it",
            authIndex < headerIndex);
    }

    /**
     * The normalization the ordering above depends on: whatever channel id the client puts on the
     * wire is discarded in favour of the one derived from the transport.
     */
    @Test
    public void headerInterceptorReplacesClientSuppliedChannelIdTest() {
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.CHANNEL_ID, "forged-by-client");

        ServerCall<Object, Object> call = mock(ServerCall.class);
        when(call.getAttributes()).thenReturn(Attributes.newBuilder()
            .set(AttributeKeys.CHANNEL_ID, "transport-channel-id")
            .build());
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);

        new HeaderInterceptor().interceptCall(call, headers, next);

        assertEquals("transport-channel-id", headers.get(GrpcConstants.CHANNEL_ID));
        verify(next).startCall(any(), any());
    }
}
