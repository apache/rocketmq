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

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;

/**
 * Records every admin RPC served by the admin gRPC server: one counter increment and one latency
 * sample per call, labeled with the method and the terminal status code, so call rate, RT and
 * error rate of the control-plane surface are visible in the same OpenTelemetry pipeline as the
 * rest of the proxy.
 *
 * <p>Registered after {@link ProxyAdminAuthInterceptor} in {@code configInterceptor(...)}; gRPC runs
 * interceptors in reverse registration order, so this one wraps the call from the outside and also
 * records calls that authentication or authorization rejects.
 *
 * <p>{@code GlobalExceptionInterceptor} sits outside this interceptor and closes the call it holds
 * directly, bypassing the {@code close} wrapper below, so a service that throws from a listener
 * callback is recorded here before the exception is rethrown to that interceptor.
 */
public class ProxyAdminMetricsInterceptor implements ServerInterceptor {

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(ServerCall<R, W> call, Metadata headers,
        ServerCallHandler<R, W> next) {
        long startNanos = System.nanoTime();
        String method = call.getMethodDescriptor().getBareMethodName() != null
            ? call.getMethodDescriptor().getBareMethodName()
            : call.getMethodDescriptor().getFullMethodName();
        // A client cancel reaches the listener while a handler may still close the call later;
        // the first terminal event wins so a call is never counted twice.
        AtomicBoolean recorded = new AtomicBoolean(false);

        ServerCall<R, W> wrapped = new ForwardingServerCall.SimpleForwardingServerCall<R, W>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                record(recorded, method, status.getCode().name(), startNanos);
                super.close(status, trailers);
            }
        };

        ServerCall.Listener<R> listener = next.startCall(wrapped, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<R>(listener) {
            @Override
            public void onMessage(R message) {
                try {
                    super.onMessage(message);
                } catch (RuntimeException | Error e) {
                    record(recorded, method, statusCodeOf(e), startNanos);
                    throw e;
                }
            }

            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (RuntimeException | Error e) {
                    record(recorded, method, statusCodeOf(e), startNanos);
                    throw e;
                }
            }

            @Override
            public void onCancel() {
                record(recorded, method, Status.CANCELLED.getCode().name(), startNanos);
                super.onCancel();
            }
        };
    }

    private static String statusCodeOf(Throwable t) {
        return t instanceof StatusRuntimeException
            ? ((StatusRuntimeException) t).getStatus().getCode().name()
            : Status.INTERNAL.getCode().name();
    }

    private static void record(AtomicBoolean recorded, String method, String status, long startNanos) {
        if (recorded.compareAndSet(false, true)) {
            ProxyMetricsManager.recordAdminCall(method, status,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
    }
}
