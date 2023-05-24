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

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class GlobalExceptionInterceptor implements ServerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    @Override
    public <R, W> ServerCall.Listener<R> interceptCall(
        ServerCall<R, W> call,
        Metadata headers,
        ServerCallHandler<R, W> next
    ) {
        final ServerCall<R, W> serverCall = new ClosableServerCall<>(call);
        ServerCall.Listener<R> delegate = next.startCall(serverCall, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<R>(delegate) {
            @Override
            public void onMessage(R message) {
                try {
                    super.onMessage(message);
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            @Override
            public void onReady() {
                try {
                    super.onReady();
                } catch (Throwable e) {
                    closeWithException(e);
                }
            }

            private void closeWithException(Throwable t) {
                Metadata trailers = new Metadata();
                Status status = Status.INTERNAL.withDescription(t.getMessage());
                boolean printLog = true;

                if (t instanceof StatusRuntimeException) {
                    trailers = ((StatusRuntimeException) t).getTrailers();
                    status = ((StatusRuntimeException) t).getStatus();
                    // no error stack for permission denied.
                    if (status.getCode().value() == Status.PERMISSION_DENIED.getCode().value()) {
                        printLog = false;
                    }
                }

                if (printLog) {
                    log.error("grpc server has exception. errorMsg:{}, e:", t.getMessage(), t);
                }

                serverCall.close(status, trailers);
            }
        };
    }

    private static class ClosableServerCall<R, W> extends
        ForwardingServerCall.SimpleForwardingServerCall<R, W> {
        private boolean closeCalled = false;

        ClosableServerCall(ServerCall<R, W> delegate) {
            super(delegate);
        }

        @Override
        public synchronized void close(final Status status, final Metadata trailers) {
            if (!closeCalled) {
                closeCalled = true;
                ClosableServerCall.super.close(status, trailers);
            }
        }
    }
}
