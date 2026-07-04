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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ProxyClientAdminEndpointHandlerTest {

    @Test
    public void handleWritesOkResultAndCompletesObserver() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                "client-a"
            ),
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(responseCaptor.getValue().getBody()).isEqualTo("client-a");
    }

    @Test
    public void handleWritesErrorResultWithoutBody() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.NOT_FOUND, "missing client"),
                null
            ),
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("missing client");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    @Test
    public void handleMapsThrownActionToStatusResponse() {
        ProxyClientAdminEndpointHandler handler = new ProxyClientAdminEndpointHandler();
        StreamObserver<TestAdminResponse> observer = mock(StreamObserver.class);

        handler.handle(
            observer,
            () -> {
                throw new IllegalArgumentException("request is required");
            },
            TestAdminResponse::new
        );

        ArgumentCaptor<TestAdminResponse> responseCaptor = ArgumentCaptor.forClass(TestAdminResponse.class);
        verify(observer).onNext(responseCaptor.capture());
        verify(observer).onCompleted();
        assertThat(responseCaptor.getValue().getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(responseCaptor.getValue().getStatus().getMessage()).contains("request is required");
        assertThat(responseCaptor.getValue().getBody()).isNull();
    }

    private static class TestAdminResponse {
        private final Status status;
        private final String body;

        private TestAdminResponse(Status status, String body) {
            this.status = status;
            this.body = body;
        }

        private Status getStatus() {
            return status;
        }

        private String getBody() {
            return body;
        }
    }
}
