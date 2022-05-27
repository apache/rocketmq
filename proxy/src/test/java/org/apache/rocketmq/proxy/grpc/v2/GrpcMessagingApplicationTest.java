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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;

@RunWith(MockitoJUnitRunner.class)
public class GrpcMessagingApplicationTest extends InitConfigAndLoggerTest {
    @Mock
    StreamObserver<QueryRouteResponse> queryRouteResponseStreamObserver;
    @Mock
    GrpcMessingActivity grpcMessingActivity;
    GrpcMessagingApplication grpcMessagingApplication;

    private static final String TOPIC = "topic";
    private static Endpoints GRPC_ENDPOINTS = Endpoints.newBuilder()
        .setScheme(AddressScheme.IPv4)
        .addAddresses(Address.newBuilder().setHost("127.0.0.1").setPort(8080).build())
        .addAddresses(Address.newBuilder().setHost("127.0.0.2").setPort(8080).build())
        .build();

    @Before
    public void setUp() throws Throwable {
        super.before();
        grpcMessagingApplication = new GrpcMessagingApplication(grpcMessingActivity);
    }

    @Test
    public void testQueryRoute() {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setEndpoints(GRPC_ENDPOINTS)
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .build();
        Mockito.when(grpcMessingActivity.queryRoute(Mockito.any(Context.class), Mockito.eq(request)))
            .thenReturn(future);
        QueryRouteResponse response = QueryRouteResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
            .addMessageQueues(MessageQueue.getDefaultInstance())
            .build();
        grpcMessagingApplication.queryRoute(request, queryRouteResponseStreamObserver);
        future.complete(response);
        await().untilAsserted(() -> {
            Mockito.verify(queryRouteResponseStreamObserver, Mockito.times(1)).onNext(response);
        });
    }
}