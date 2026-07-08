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

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.Status;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyClientAdminEndpointIntegrationTest extends InitConfigTest {
    private static final QueryRouteRequest PROTO_REQUEST = QueryRouteRequest.getDefaultInstance();

    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ProxyRelayService proxyRelayService;

    @Before
    public void setUp() throws Throwable {
        super.before();
        when(this.messagingProcessor.getProxyRelayService()).thenReturn(this.proxyRelayService);
    }

    @Test
    public void publicEndpointReadyExecutorQueriesSharedReadModelForAllRpcShapes() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        try {
            ProxyClientReadService readService = readService(activity);
            readService.upsertClient(client("client-a", ClientType.PRODUCER, "group-a", "topic-a", "JAVA", 100L));
            readService.upsertClient(client(
                "client-b",
                ClientType.PUSH_CONSUMER,
                "group-b",
                "topic-b",
                "CPP",
                200L
            ));
            ProxyClientAdminEndpointExecutor executor = activity.getProxyClientAdminEndpointExecutor();

            TestAdminResponse listClients = listClients(
                executor,
                ProxyClientAdminListClientsRequest.newBuilder()
                    .setPageSize(100)
                    .build()
            );
            assertThat(listClients.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(((ProxyClientAdminPageView) listClients.getBody()).getClients())
                .extracting(ProxyClientAdminClientView::getClientId)
                .containsExactly("client-a", "client-b");

            TestAdminResponse describeClient = describeClient(
                executor,
                ProxyClientAdminDescribeClientRequest.newBuilder()
                    .setClientId("client-a")
                    .build()
            );
            assertThat(describeClient.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(((ProxyClientAdminClientView) describeClient.getBody()).getClientId()).isEqualTo("client-a");

            TestAdminResponse listClientsByGroup = listClientsByGroup(
                executor,
                ProxyClientAdminListClientsByGroupRequest.newBuilder()
                    .setGroup("group-a")
                    .setPageSize(100)
                    .build()
            );
            assertThat(listClientsByGroup.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(((ProxyClientAdminPageView) listClientsByGroup.getBody()).getClients())
                .extracting(ProxyClientAdminClientView::getClientId)
                .containsExactly("client-a");

            TestAdminResponse listClientsByTopic = listClientsByTopic(
                executor,
                ProxyClientAdminListClientsByTopicRequest.newBuilder()
                    .setTopic("topic-b")
                    .setPageSize(100)
                    .build()
            );
            assertThat(listClientsByTopic.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(((ProxyClientAdminPageView) listClientsByTopic.getBody()).getClients())
                .extracting(ProxyClientAdminClientView::getClientId)
                .containsExactly("client-b");
        } finally {
            activity.shutdown();
        }
    }

    @Test
    public void publicEndpointReadyExecutorMapsNotFoundThroughSharedChain() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        try {
            TestAdminResponse response = describeClient(
                activity.getProxyClientAdminEndpointExecutor(),
                ProxyClientAdminDescribeClientRequest.newBuilder()
                    .setClientId("missing-client")
                    .build()
            );

            assertThat(response.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
            assertThat(response.getBody()).isNull();
        } finally {
            activity.shutdown();
        }
    }

    @Test
    public void publicEndpointReadyExecutorMapsMissingAuthSubjectToUnauthorized() throws Exception {
        ConfigurationManager.getAuthConfig().setAuthorizationEnabled(true);
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        try {
            TestAdminResponse response = listClients(
                activity.getProxyClientAdminEndpointExecutor(),
                ProxyClientAdminListClientsRequest.newBuilder()
                    .setPageSize(100)
                    .build()
            );

            assertThat(response.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
            assertThat(response.getBody()).isNull();
        } finally {
            activity.shutdown();
        }
    }

    private static TestAdminResponse listClients(ProxyClientAdminEndpointExecutor executor,
        ProxyClientAdminListClientsRequest request) throws Exception {
        CapturingObserver observer = new CapturingObserver();
        executor.listClients(headers(), PROTO_REQUEST, ignored -> request, observer, TestAdminResponse::new);
        return observer.awaitResponse();
    }

    private static TestAdminResponse describeClient(ProxyClientAdminEndpointExecutor executor,
        ProxyClientAdminDescribeClientRequest request) throws Exception {
        CapturingObserver observer = new CapturingObserver();
        executor.describeClient(headers(), PROTO_REQUEST, ignored -> request, observer, TestAdminResponse::new);
        return observer.awaitResponse();
    }

    private static TestAdminResponse listClientsByGroup(ProxyClientAdminEndpointExecutor executor,
        ProxyClientAdminListClientsByGroupRequest request) throws Exception {
        CapturingObserver observer = new CapturingObserver();
        executor.listClientsByGroup(headers(), PROTO_REQUEST, ignored -> request, observer, TestAdminResponse::new);
        return observer.awaitResponse();
    }

    private static TestAdminResponse listClientsByTopic(ProxyClientAdminEndpointExecutor executor,
        ProxyClientAdminListClientsByTopicRequest request) throws Exception {
        CapturingObserver observer = new CapturingObserver();
        executor.listClientsByTopic(headers(), PROTO_REQUEST, ignored -> request, observer, TestAdminResponse::new);
        return observer.awaitResponse();
    }

    private static Metadata headers() {
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.REMOTE_ADDRESS, "127.0.0.1:8080");
        headers.put(GrpcConstants.LOCAL_ADDRESS, "192.168.0.1:8080");
        headers.put(GrpcConstants.AUTHORIZATION_AK, "admin");
        return headers;
    }

    private static ProxyClientReadService readService(DefaultGrpcMessagingActivity activity) throws Exception {
        Field field = DefaultGrpcMessagingActivity.class.getDeclaredField("proxyClientReadService");
        field.setAccessible(true);
        return (ProxyClientReadService) field.get(activity);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, String group, String topic,
        String language, long connectTimeMillis) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            Collections.singleton(group),
            Collections.singleton(topic),
            language,
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            connectTimeMillis,
            connectTimeMillis + 10L
        );
    }

    private static class CapturingObserver implements StreamObserver<TestAdminResponse> {
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicReference<TestAdminResponse> response = new AtomicReference<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override
        public void onNext(TestAdminResponse value) {
            this.response.set(value);
        }

        @Override
        public void onError(Throwable t) {
            this.error.set(t);
            this.completed.countDown();
        }

        @Override
        public void onCompleted() {
            this.completed.countDown();
        }

        private TestAdminResponse awaitResponse() throws Exception {
            assertThat(this.completed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(this.error.get()).isNull();
            assertThat(this.response.get()).isNotNull();
            return this.response.get();
        }
    }

    private static class TestAdminResponse {
        private final Status status;
        private final Object body;

        private TestAdminResponse(Status status, Object body) {
            this.status = status;
            this.body = body;
        }

        private Status getStatus() {
            return status;
        }

        private Object getBody() {
            return body;
        }
    }
}
