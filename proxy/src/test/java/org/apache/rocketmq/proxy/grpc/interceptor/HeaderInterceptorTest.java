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

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.net.InetSocketAddress;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HeaderInterceptorTest {

    @Test
    public void testInterceptCallWithUnresolvedSocketAddress() {
        HeaderInterceptor interceptor = new HeaderInterceptor();
        Metadata headers = new Metadata();
        Attributes attributes = Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, InetSocketAddress.createUnresolved("remote.example.com", 8081))
            .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, InetSocketAddress.createUnresolved("local.example.com", 8080))
            .build();

        ServerCall<String, String> call = new TestServerCall(attributes);
        ServerCallHandler<String, String> handler = (serverCall, metadata) -> new ServerCall.Listener<String>() {
        };

        interceptor.interceptCall(call, headers, handler);

        assertEquals("remote.example.com:8081", headers.get(GrpcConstants.REMOTE_ADDRESS));
        assertEquals("local.example.com:8080", headers.get(GrpcConstants.LOCAL_ADDRESS));
    }

    private static class TestServerCall extends ServerCall<String, String> {
        private final Attributes attributes;

        private TestServerCall(Attributes attributes) {
            this.attributes = attributes;
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void sendHeaders(Metadata headers) {
        }

        @Override
        public void sendMessage(String message) {
        }

        @Override
        public void close(Status status, Metadata trailers) {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }

        @Override
        public MethodDescriptor<String, String> getMethodDescriptor() {
            return null;
        }
    }
}
