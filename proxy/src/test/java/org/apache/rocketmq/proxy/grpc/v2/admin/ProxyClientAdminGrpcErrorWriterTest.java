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

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletionException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyClientAdminGrpcErrorWriterTest {

    @Test
    public void writeUsesInternalErrorWhenThrowableIsMissing() {
        CapturingStreamObserver responseObserver = new CapturingStreamObserver();

        ProxyClientAdminGrpcErrorWriter.write(responseObserver, null);

        assertThat(responseObserver.error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException error = (StatusRuntimeException) responseObserver.error;
        assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
        assertThat(error.getStatus().getDescription()).contains("proxy admin grpc error is required");
    }

    @Test
    public void writePreservesExplicitGrpcStatusRuntimeException() {
        CapturingStreamObserver responseObserver = new CapturingStreamObserver();
        StatusRuntimeException expectedError =
            io.grpc.Status.INVALID_ARGUMENT.withDescription("bad peer request").asRuntimeException();

        ProxyClientAdminGrpcErrorWriter.write(responseObserver, expectedError);

        assertThat(responseObserver.error).isSameAs(expectedError);
    }

    @Test
    public void writePreservesWrappedExplicitGrpcStatusRuntimeException() {
        CapturingStreamObserver responseObserver = new CapturingStreamObserver();
        StatusRuntimeException expectedError =
            io.grpc.Status.INVALID_ARGUMENT.withDescription("wrapped bad peer request").asRuntimeException();

        ProxyClientAdminGrpcErrorWriter.write(responseObserver, new CompletionException(expectedError));

        assertThat(responseObserver.error).isSameAs(expectedError);
    }

    private static class CapturingStreamObserver implements StreamObserver<Object> {
        private Throwable error;

        @Override
        public void onNext(Object value) {
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
