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

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * RIP-2 acceptance criteria #4: measures transport-level failures of admin RPCs
 * (authentication rejections, permission denials, framework errors) and feeds
 * {@link ProxyAdminMetricsManager}. Business-level success/error outcomes are
 * recorded by the admin services themselves, because they complete the gRPC
 * call with an OK status and carry the error inside the response payload.
 */
public class ProxyAdminMetricsInterceptor implements ServerInterceptor {

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(ServerCall<R, W> call, Metadata headers,
        ServerCallHandler<R, W> next) {
        final String method = call.getMethodDescriptor().getBareMethodName();
        final long startNanos = System.nanoTime();
        ServerCall<R, W> observedCall = new SimpleForwardingServerCall<R, W>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                if (!status.isOk()) {
                    long latencyMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    ProxyAdminMetricsManager.recordError(method, latencyMillis, status.asRuntimeException());
                }
                super.close(status, trailers);
            }
        };
        return next.startCall(observedCall, headers);
    }
}
