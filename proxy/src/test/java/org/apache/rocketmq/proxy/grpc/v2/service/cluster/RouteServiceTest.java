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

package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import com.google.common.net.HostAndPort;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.connector.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyMode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class RouteServiceTest extends BaseServiceTest {
    private String brokerAddress = "127.0.0.1:10911";
    public static final String BROKER_NAME = "brokerName";
    public static final String NAMESPACE = "namespace";
    public static final String TOPIC = "topic";
    public static final Broker MOCK_BROKER = Broker.newBuilder().setName(BROKER_NAME).build();
    public static final Resource MOCK_TOPIC = Resource.newBuilder()
        .setName(TOPIC)
        .setResourceNamespace(NAMESPACE)
        .build();

    private static final Settings WITH_HOST_SETTINGS = Settings.newBuilder()
        .setAccessPoint(Endpoints.newBuilder()
            .addAddresses(Address.newBuilder()
                .setPort(80)
                .setHost("host")
                .build())
            .setScheme(AddressScheme.DOMAIN_NAME)
            .build())
        .build();

    private static final Settings INVALID_HOST_SETTINGS = Settings.newBuilder()
        .setAccessPoint(Endpoints.getDefaultInstance())
        .build();

    @Override
    public void beforeEach() throws Exception {
        TopicRouteData routeData = new TopicRouteData();

        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("cluster");
        brokerData.setBrokerName("brokerName");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>() {{
            put(0L, brokerAddress);
        }};
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);

        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setPerm(6);
        queueData.setWriteQueueNums(8);
        queueData.setReadQueueNums(8);
        queueData.setBrokerName("brokerName");
        queueDataList.add(queueData);

        routeData.setBrokerDatas(brokerDataList);
        routeData.setQueueDatas(queueDataList);

        MessageQueueWrapper messageQueueWrapper = new MessageQueueWrapper("topic", routeData);
        when(this.topicRouteCache.getMessageQueue("topic")).thenReturn(messageQueueWrapper);

        when(this.topicRouteCache.getMessageQueue("notExistTopic")).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));
    }

    @Test
    public void testGenPartitionFromQueueData() throws Exception {
        // test queueData with 8 read queues, 8 write queues, and rw permission, expect 8 rw queues.
        QueueData queueDataWith8R8WPermRW = mockQueueData(8, 8, PermName.PERM_READ | PermName.PERM_WRITE);
        List<MessageQueue> partitionWith8R8WPermRW = GrpcConverter.genMessageQueueFromQueueData(queueDataWith8R8WPermRW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R8WPermRW.size()).isEqualTo(8);
        assertThat(partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(8);
        assertThat(partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(0);
        assertThat(partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(0);

        // test queueData with 8 read queues, 8 write queues, and read only permission, expect 8 read only queues.
        QueueData queueDataWith8R8WPermR = mockQueueData(8, 8, PermName.PERM_READ);
        List<MessageQueue> partitionWith8R8WPermR = GrpcConverter.genMessageQueueFromQueueData(queueDataWith8R8WPermR, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R8WPermR.size()).isEqualTo(8);
        assertThat(partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(8);
        assertThat(partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(0);
        assertThat(partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(0);

        // test queueData with 8 read queues, 8 write queues, and write only permission, expect 8 write only queues.
        QueueData queueDataWith8R8WPermW = mockQueueData(8, 8, PermName.PERM_WRITE);
        List<MessageQueue> partitionWith8R8WPermW = GrpcConverter.genMessageQueueFromQueueData(queueDataWith8R8WPermW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R8WPermW.size()).isEqualTo(8);
        assertThat(partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(8);
        assertThat(partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(0);
        assertThat(partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(0);

        // test queueData with 8 read queues, 0 write queues, and rw permission, expect 8 read only queues.
        QueueData queueDataWith8R0WPermRW = mockQueueData(8, 0, PermName.PERM_READ | PermName.PERM_WRITE);
        List<MessageQueue> partitionWith8R0WPermRW = GrpcConverter.genMessageQueueFromQueueData(queueDataWith8R0WPermRW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith8R0WPermRW.size()).isEqualTo(8);
        assertThat(partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(8);
        assertThat(partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(0);
        assertThat(partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(0);

        // test queueData with 4 read queues, 8 write queues, and rw permission, expect 4 rw queues and  4 write only queues.
        QueueData queueDataWith4R8WPermRW = mockQueueData(4, 8, PermName.PERM_READ | PermName.PERM_WRITE);
        List<MessageQueue> partitionWith4R8WPermRW = GrpcConverter.genMessageQueueFromQueueData(queueDataWith4R8WPermRW, MOCK_TOPIC, MOCK_BROKER);
        assertThat(partitionWith4R8WPermRW.size()).isEqualTo(8);
        assertThat(partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count()).isEqualTo(4);
        assertThat(partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count()).isEqualTo(4);
        assertThat(partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count()).isEqualTo(0);

    }

    private QueueData mockQueueData(int r, int w, int perm) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(BROKER_NAME);
        queueData.setReadQueueNums(r);
        queueData.setWriteQueueNums(w);
        queueData.setPerm(perm);
        return queueData;
    }

    @Test
    public void testLocalModeQueryRoute() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.LOCAL, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(WITH_HOST_SETTINGS);

        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .build());
        QueryRouteResponse response = future.get();
        assertEquals(Code.OK.getNumber(), response.getStatus().getCode().getNumber());
        assertEquals(8, response.getMessageQueuesCount());
        assertEquals(HostAndPort.fromString(brokerAddress).getHost(), response.getMessageQueues(0).getBroker()
            .getEndpoints().getAddresses(0).getHost());
    }

    @Test
    public void testQueryRouteWithInvalidEndpoints() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.CLUSTER, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(INVALID_HOST_SETTINGS);
        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .build());

        QueryRouteResponse response = future.get();
        assertEquals(Code.ILLEGAL_ACCESS_POINT.getNumber(), response.getStatus().getCode().getNumber());
    }

    @Test
    public void testQueryRoute() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.CLUSTER, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(WITH_HOST_SETTINGS);

        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .build());

        QueryRouteResponse response = future.get();
        assertEquals(Code.OK.getNumber(), response.getStatus().getCode().getNumber());
        assertEquals(8, response.getMessageQueuesCount());
        assertEquals("host", response.getMessageQueues(0).getBroker()
            .getEndpoints().getAddresses(0).getHost());
    }

    @Test
    public void testQueryRouteWhenTopicNotExist() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.CLUSTER, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(WITH_HOST_SETTINGS);

        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("notExistTopic")
                .build())
            .build());

        QueryRouteResponse response = future.get();
        assertEquals(Code.TOPIC_NOT_FOUND.getNumber(), response.getStatus().getCode().getNumber());
    }

    @Test
    public void testQueryAssignmentInvalidEndpoints() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.CLUSTER, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(INVALID_HOST_SETTINGS);
        CompletableFuture<QueryAssignmentResponse> future = routeService.queryAssignment(Context.current(), QueryAssignmentRequest.newBuilder()
            .setTopic(
                Resource.newBuilder()
                    .setName("topic")
                    .build()
            )
            .build());

        QueryAssignmentResponse response = future.get();
        assertEquals(Code.ILLEGAL_ACCESS_POINT.getNumber(), response.getStatus().getCode().getNumber());
    }

    @Test
    public void testLocalModeQueryAssignment() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.LOCAL, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(WITH_HOST_SETTINGS);

        CompletableFuture<QueryAssignmentResponse> future = routeService.queryAssignment(Context.current(), QueryAssignmentRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .build());

        QueryAssignmentResponse response = future.get();
        assertEquals(Code.OK.getNumber(), response.getStatus().getCode().getNumber());
        assertEquals(1, response.getAssignmentsCount());
        assertEquals("brokerName", response.getAssignments(0).getMessageQueue().getBroker().getName());
        assertEquals(HostAndPort.fromString(brokerAddress).getHost(), response.getAssignments(0).getMessageQueue().getBroker().getEndpoints().getAddresses(0).getHost());
    }

    @Test
    public void testQueryAssignment() throws Exception {
        RouteService routeService = new RouteService(ProxyMode.CLUSTER, this.connectorManager, this.grpcClientManager);

        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(WITH_HOST_SETTINGS);

        CompletableFuture<QueryAssignmentResponse> future = routeService.queryAssignment(Context.current(), QueryAssignmentRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .build());

        QueryAssignmentResponse response = future.get();
        assertEquals(Code.OK.getNumber(), response.getStatus().getCode().getNumber());
        assertEquals(1, response.getAssignmentsCount());
        assertEquals("brokerName", response.getAssignments(0).getMessageQueue().getBroker().getName());
        assertEquals("host", response.getAssignments(0).getMessageQueue().getBroker().getEndpoints().getAddresses(0).getHost());
    }

}
