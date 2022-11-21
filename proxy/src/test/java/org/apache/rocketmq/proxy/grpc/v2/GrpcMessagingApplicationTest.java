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
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class GrpcMessagingApplicationTest extends InitConfigTest {
    protected static final String REMOTE_ADDR = "192.168.0.1:8080";
    protected static final String LOCAL_ADDR = "127.0.0.1:8080";
    protected static final String CLIENT_ID = "client-id" + UUID.randomUUID();
    protected static final String JAVA = "JAVA";
    @Mock
    StreamObserver<QueryRouteResponse> queryRouteResponseStreamObserver;
    @Mock
    GrpcMessingActivity grpcMessingActivity;
    GrpcMessagingApplication grpcMessagingApplication;

    private static final String TOPIC = "topic";
    private static Endpoints grpcEndpoints = Endpoints.newBuilder()
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
        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.CLIENT_ID, CLIENT_ID);
        metadata.put(InterceptorConstants.LANGUAGE, JAVA);
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, REMOTE_ADDR);
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, LOCAL_ADDR);
        
        Assert.assertNotNull(Context.current()
            .withValue(InterceptorConstants.METADATA, metadata)
            .attach());

        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setEndpoints(grpcEndpoints)
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .build();
        Mockito.when(grpcMessingActivity.queryRoute(Mockito.any(ProxyContext.class), Mockito.eq(request)))
            .thenReturn(future);
        QueryRouteResponse response = QueryRouteResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .addMessageQueues(MessageQueue.getDefaultInstance())
            .build();
        grpcMessagingApplication.queryRoute(request, queryRouteResponseStreamObserver);
        future.complete(response);
        await().untilAsserted(() -> {
            Mockito.verify(queryRouteResponseStreamObserver, Mockito.times(1)).onNext(Mockito.same(response));
        });
    }

    @Test
    public void testQueryRouteWithBadClientID() {
        Metadata metadata = new Metadata();
        metadata.put(InterceptorConstants.LANGUAGE, JAVA);
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, REMOTE_ADDR);
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, LOCAL_ADDR);

        Assert.assertNotNull(Context.current()
            .withValue(InterceptorConstants.METADATA, metadata)
            .attach());

        QueryRouteRequest request = QueryRouteRequest.newBuilder()
            .setEndpoints(grpcEndpoints)
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .build();
        grpcMessagingApplication.queryRoute(request, queryRouteResponseStreamObserver);

        ArgumentCaptor<QueryRouteResponse> responseArgumentCaptor = ArgumentCaptor.forClass(QueryRouteResponse.class);
        await().untilAsserted(() -> {
            Mockito.verify(queryRouteResponseStreamObserver, Mockito.times(1)).onNext(responseArgumentCaptor.capture());
        });

        assertEquals(Code.CLIENT_ID_REQUIRED, responseArgumentCaptor.getValue().getStatus().getCode());
    }
}