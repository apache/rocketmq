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
package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import io.grpc.Status;
import java.util.NoSuchElementException;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ResponseBuilderTest {

    @Test
    public void buildStatusMapsIllegalArgumentExceptionToBadRequest() {
        assertThat(ResponseBuilder.getInstance().buildStatus(
            new IllegalArgumentException("clientId is required")).getCode())
            .isEqualTo(Code.BAD_REQUEST);
    }

    @Test
    public void buildStatusMapsNoSuchElementExceptionToNotFound() {
        assertThat(ResponseBuilder.getInstance().buildStatus(
            new NoSuchElementException("Client not found: client-a")).getCode())
            .isEqualTo(Code.NOT_FOUND);
    }

    @Test
    public void buildStatusMapsAuthenticationExceptionToUnauthorized() {
        assertThat(ResponseBuilder.getInstance().buildStatus(
            new AuthenticationException("authentication failed")).getCode())
            .isEqualTo(Code.UNAUTHORIZED);
    }

    @Test
    public void buildStatusMapsAuthorizationExceptionToUnauthorized() {
        assertThat(ResponseBuilder.getInstance().buildStatus(
            new AuthorizationException("authorization failed")).getCode())
            .isEqualTo(Code.UNAUTHORIZED);
    }

    @Test
    public void buildStatusMapsGrpcRuntimeStatusExceptionToRocketMqStatus() {
        assertGrpcStatusMapsToRocketMqCode(Status.INVALID_ARGUMENT.withDescription("bad admin request"),
            Code.BAD_REQUEST, "bad admin request");
        assertGrpcStatusMapsToRocketMqCode(Status.NOT_FOUND.withDescription("peer not found"),
            Code.NOT_FOUND, "peer not found");
        assertGrpcStatusMapsToRocketMqCode(Status.PERMISSION_DENIED.withDescription("admin denied"),
            Code.UNAUTHORIZED, "admin denied");
        assertGrpcStatusMapsToRocketMqCode(Status.RESOURCE_EXHAUSTED.withDescription("admin throttled"),
            Code.TOO_MANY_REQUESTS, "admin throttled");
        assertGrpcStatusMapsToRocketMqCode(Status.UNIMPLEMENTED.withDescription("admin not implemented"),
            Code.NOT_IMPLEMENTED, "admin not implemented");
        assertGrpcStatusMapsToRocketMqCode(Status.UNAVAILABLE.withDescription("peer unavailable"),
            Code.PROXY_TIMEOUT, "peer unavailable");
        assertGrpcStatusMapsToRocketMqCode(Status.DEADLINE_EXCEEDED.withDescription("peer deadline exceeded"),
            Code.PROXY_TIMEOUT, "peer deadline exceeded");
    }

    @Test
    public void buildStatusMapsGrpcCheckedStatusExceptionToRocketMqStatus() {
        assertThat(ResponseBuilder.getInstance().buildStatus(
            Status.UNAVAILABLE.withDescription("checked peer unavailable").asException()).getCode())
            .isEqualTo(Code.PROXY_TIMEOUT);
    }

    private static void assertGrpcStatusMapsToRocketMqCode(Status grpcStatus, Code expectedCode,
        String expectedMessage) {
        apache.rocketmq.v2.Status status = ResponseBuilder.getInstance().buildStatus(grpcStatus.asRuntimeException());

        assertThat(status.getCode()).isEqualTo(expectedCode);
        assertThat(status.getMessage()).contains(expectedMessage);
    }
}
