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

package org.apache.rocketmq.proxy.grpc.v2.route;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import com.google.common.net.HostAndPort;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.metadata.LocalMetadataService;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class RouteActivityTest extends BaseActivityTest {

    private RouteActivity routeActivity;

    private static final String CLUSTER = "cluster";
    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final String BROKER_NAME = "brokerName";
    private static final Broker GRPC_BROKER = Broker.newBuilder().setName(BROKER_NAME).build();
    private static final Resource GRPC_TOPIC = Resource.newBuilder()
        .setName(TOPIC)
        .build();
    private static final Resource GRPC_GROUP = Resource.newBuilder()
        .setName(GROUP)
        .build();
    private static Endpoints grpcEndpoints = Endpoints.newBuilder()
        .setScheme(AddressScheme.IPv4)
        .addAddresses(Address.newBuilder().setHost("127.0.0.1").setPort(8080).build())
        .addAddresses(Address.newBuilder().setHost("127.0.0.2").setPort(8080).build())
        .build();
    private static List<org.apache.rocketmq.proxy.common.Address> addressArrayList = new ArrayList<>();

    static {
        addressArrayList.add(new org.apache.rocketmq.proxy.common.Address(
            org.apache.rocketmq.proxy.common.Address.AddressScheme.IPv4,
            HostAndPort.fromParts("127.0.0.1", 8080)));
        addressArrayList.add(new org.apache.rocketmq.proxy.common.Address(
            org.apache.rocketmq.proxy.common.Address.AddressScheme.IPv4,
            HostAndPort.fromParts("127.0.0.2", 8080)));
    }

    @Before
    public void before() throws Throwable {
        super.before();
        this.routeActivity = new RouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testQueryRoute() throws Throwable {
        ConfigurationManager.getProxyConfig().setGrpcServerPort(8080);
        ArgumentCaptor<List<org.apache.rocketmq.proxy.common.Address>> addressListCaptor = ArgumentCaptor.forClass(List.class);
        when(this.messagingProcessor.getTopicRouteDataForProxy(any(), addressListCaptor.capture(), anyString()))
            .thenReturn(createProxyTopicRouteData(2, 2, 6));
        MetadataService metadataService = Mockito.mock(LocalMetadataService.class);
        when(this.messagingProcessor.getMetadataService()).thenReturn(metadataService);
        when(metadataService.getTopicMessageType(any(), anyString())).thenReturn(TopicMessageType.NORMAL);

        QueryRouteResponse response = this.routeActivity.queryRoute(
            createContext(),
            QueryRouteRequest.newBuilder()
                .setEndpoints(grpcEndpoints)
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(4, response.getMessageQueuesCount());
        for (MessageQueue messageQueue : response.getMessageQueuesList()) {
            assertEquals(grpcEndpoints, messageQueue.getBroker().getEndpoints());
            assertEquals(Permission.READ_WRITE, messageQueue.getPermission());
        }
    }

    @Test
    public void testQueryRouteTopicExist() throws Throwable {
        when(this.messagingProcessor.getTopicRouteDataForProxy(any(), any(), anyString()))
            .thenThrow(new MQBrokerException(ResponseCode.TOPIC_NOT_EXIST, ""));

        try {
            this.routeActivity.queryRoute(
                createContext(),
                QueryRouteRequest.newBuilder()
                    .setEndpoints(grpcEndpoints)
                    .setTopic(GRPC_TOPIC)
                    .build()
            ).get();
        } catch (Throwable t) {
            assertEquals(Code.TOPIC_NOT_FOUND, ResponseBuilder.getInstance().buildStatus(t).getCode());
            return;
        }
        fail();
    }

    @Test
    public void testQueryAssignmentWithNoReadPerm() throws Throwable {
        when(this.messagingProcessor.getTopicRouteDataForProxy(any(), any(), anyString()))
            .thenReturn(createProxyTopicRouteData(2, 2, PermName.PERM_WRITE));

        QueryAssignmentResponse response = this.routeActivity.queryAssignment(
            createContext(),
            QueryAssignmentRequest.newBuilder()
                .setEndpoints(grpcEndpoints)
                .setTopic(GRPC_TOPIC)
                .setGroup(GRPC_GROUP)
                .build()
        ).get();

        assertEquals(Code.FORBIDDEN, response.getStatus().getCode());
    }

    @Test
    public void testQueryAssignmentWithNoReadQueue() throws Throwable {
        when(this.messagingProcessor.getTopicRouteDataForProxy(any(), any(), anyString()))
            .thenReturn(createProxyTopicRouteData(0, 2, 6));

        QueryAssignmentResponse response = this.routeActivity.queryAssignment(
            createContext(),
            QueryAssignmentRequest.newBuilder()
                .setEndpoints(grpcEndpoints)
                .setTopic(GRPC_TOPIC)
                .setGroup(GRPC_GROUP)
                .build()
        ).get();

        assertEquals(Code.FORBIDDEN, response.getStatus().getCode());
    }

    @Test
    public void testQueryAssignment() throws Throwable {
        when(this.messagingProcessor.getTopicRouteDataForProxy(any(), any(), anyString()))
            .thenReturn(createProxyTopicRouteData(2, 2, 6));

        QueryAssignmentResponse response = this.routeActivity.queryAssignment(
            createContext(),
            QueryAssignmentRequest.newBuilder()
                .setEndpoints(grpcEndpoints)
                .setTopic(GRPC_TOPIC)
                .setGroup(GRPC_GROUP)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getAssignmentsCount());
        assertEquals(grpcEndpoints, response.getAssignments(0).getMessageQueue().getBroker().getEndpoints());
    }

    @Test
    public void testQueryFifoAssignment() throws Throwable {
        when(this.messagingProcessor.getTopicRouteDataForProxy(any(), any(), anyString()))
            .thenReturn(createProxyTopicRouteData(2, 2, 6));
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setConsumeMessageOrderly(true);
        when(this.messagingProcessor.getSubscriptionGroupConfig(any(), anyString())).thenReturn(subscriptionGroupConfig);

        QueryAssignmentResponse response = this.routeActivity.queryAssignment(
            createContext(),
            QueryAssignmentRequest.newBuilder()
                .setEndpoints(grpcEndpoints)
                .setTopic(GRPC_TOPIC)
                .setGroup(GRPC_GROUP)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(2, response.getAssignmentsCount());
        assertEquals(grpcEndpoints, response.getAssignments(0).getMessageQueue().getBroker().getEndpoints());
    }

    private static ProxyTopicRouteData createProxyTopicRouteData(int r, int w, int p) {
        ProxyTopicRouteData proxyTopicRouteData = new ProxyTopicRouteData();
        proxyTopicRouteData.getQueueDatas().add(createQueueData(r, w, p));
        ProxyTopicRouteData.ProxyBrokerData proxyBrokerData = new ProxyTopicRouteData.ProxyBrokerData();
        proxyBrokerData.setCluster(CLUSTER);
        proxyBrokerData.setBrokerName(BROKER_NAME);
        proxyBrokerData.getBrokerAddrs().put(0L, addressArrayList);
        proxyBrokerData.getBrokerAddrs().put(1L, addressArrayList);
        proxyTopicRouteData.getBrokerDatas().add(proxyBrokerData);
        return proxyTopicRouteData;
    }

    @Test
    public void testGenPartitionFromQueueData() throws Exception {
        // test queueData with 8 read queues, 8 write queues, and rw permission, expect 8 rw queues.
        QueueData queueDataWith8R8WPermRW = createQueueData(8, 8, PermName.PERM_READ | PermName.PERM_WRITE);
        List<MessageQueue> partitionWith8R8WPermRW = this.routeActivity.genMessageQueueFromQueueData(queueDataWith8R8WPermRW, GRPC_TOPIC, TopicMessageType.NORMAL, GRPC_BROKER);
        assertEquals(8, partitionWith8R8WPermRW.size());
        assertEquals(8, partitionWith8R8WPermRW.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.NORMAL.getNumber()).count());
        assertEquals(8, partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count());
        assertEquals(0, partitionWith8R8WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count());

        // test queueData with 8 read queues, 8 write queues, and read only permission, expect 8 read only queues.
        QueueData queueDataWith8R8WPermR = createQueueData(8, 8, PermName.PERM_READ);
        List<MessageQueue> partitionWith8R8WPermR = this.routeActivity.genMessageQueueFromQueueData(queueDataWith8R8WPermR, GRPC_TOPIC, TopicMessageType.FIFO, GRPC_BROKER);
        assertEquals(8, partitionWith8R8WPermR.size());
        assertEquals(8, partitionWith8R8WPermR.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.FIFO.getNumber()).count());
        assertEquals(8, partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.READ).count());
        assertEquals(0, partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith8R8WPermR.stream().filter(a -> a.getPermission() == Permission.WRITE).count());

        // test queueData with 8 read queues, 8 write queues, and write only permission, expect 8 write only queues.
        QueueData queueDataWith8R8WPermW = createQueueData(8, 8, PermName.PERM_WRITE);
        List<MessageQueue> partitionWith8R8WPermW = this.routeActivity.genMessageQueueFromQueueData(queueDataWith8R8WPermW, GRPC_TOPIC, TopicMessageType.TRANSACTION, GRPC_BROKER);
        assertEquals(8, partitionWith8R8WPermW.size());
        assertEquals(8, partitionWith8R8WPermW.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.TRANSACTION.getNumber()).count());
        assertEquals(8, partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.WRITE).count());
        assertEquals(0, partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith8R8WPermW.stream().filter(a -> a.getPermission() == Permission.READ).count());

        // test queueData with 8 read queues, 0 write queues, and rw permission, expect 8 read only queues.
        QueueData queueDataWith8R0WPermRW = createQueueData(8, 0, PermName.PERM_READ | PermName.PERM_WRITE);
        List<MessageQueue> partitionWith8R0WPermRW = this.routeActivity.genMessageQueueFromQueueData(queueDataWith8R0WPermRW, GRPC_TOPIC, TopicMessageType.DELAY, GRPC_BROKER);
        assertEquals(8, partitionWith8R0WPermRW.size());
        assertEquals(8, partitionWith8R0WPermRW.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.DELAY.getNumber()).count());
        assertEquals(8, partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count());
        assertEquals(0, partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith8R0WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count());

        // test queueData with 4 read queues, 8 write queues, and rw permission, expect 4 rw queues and  4 write only queues.
        QueueData queueDataWith4R8WPermRW = createQueueData(4, 8, PermName.PERM_READ | PermName.PERM_WRITE);
        List<MessageQueue> partitionWith4R8WPermRW = this.routeActivity.genMessageQueueFromQueueData(queueDataWith4R8WPermRW, GRPC_TOPIC, TopicMessageType.UNSPECIFIED, GRPC_BROKER);
        assertEquals(8, partitionWith4R8WPermRW.size());
        assertEquals(8, partitionWith4R8WPermRW.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.MESSAGE_TYPE_UNSPECIFIED.getNumber()).count());
        assertEquals(4, partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.WRITE).count());
        assertEquals(4, partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith4R8WPermRW.stream().filter(a -> a.getPermission() == Permission.READ).count());

        // test queueData with 2 read queues, 2 write queues, and no permission, expect 2 no permission queues.
        QueueData queueDataWith2R2WNoPerm = createQueueData(2, 2, 0);
        List<MessageQueue> partitionWith2R2WNoPerm = this.routeActivity.genMessageQueueFromQueueData(queueDataWith2R2WNoPerm, GRPC_TOPIC, TopicMessageType.UNSPECIFIED, GRPC_BROKER);
        assertEquals(2, partitionWith2R2WNoPerm.size());
        assertEquals(2, partitionWith2R2WNoPerm.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.MESSAGE_TYPE_UNSPECIFIED.getNumber()).count());
        assertEquals(2, partitionWith2R2WNoPerm.stream().filter(a -> a.getPermission() == Permission.NONE).count());
        assertEquals(0, partitionWith2R2WNoPerm.stream().filter(a -> a.getPermission() == Permission.WRITE).count());
        assertEquals(0, partitionWith2R2WNoPerm.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith2R2WNoPerm.stream().filter(a -> a.getPermission() == Permission.READ).count());

        // test queueData with 0 read queues, 0 write queues, and no permission, expect 1 no permission queue.
        QueueData queueDataWith0R0WNoPerm = createQueueData(0, 0, 0);
        List<MessageQueue> partitionWith0R0WNoPerm = this.routeActivity.genMessageQueueFromQueueData(queueDataWith0R0WNoPerm, GRPC_TOPIC, TopicMessageType.UNSPECIFIED, GRPC_BROKER);
        assertEquals(1, partitionWith0R0WNoPerm.size());
        assertEquals(1, partitionWith0R0WNoPerm.stream().filter(a -> a.getAcceptMessageTypesValue(0) == MessageType.MESSAGE_TYPE_UNSPECIFIED.getNumber()).count());
        assertEquals(1, partitionWith0R0WNoPerm.stream().filter(a -> a.getPermission() == Permission.NONE).count());
        assertEquals(0, partitionWith0R0WNoPerm.stream().filter(a -> a.getPermission() == Permission.WRITE).count());
        assertEquals(0, partitionWith0R0WNoPerm.stream().filter(a -> a.getPermission() == Permission.READ_WRITE).count());
        assertEquals(0, partitionWith0R0WNoPerm.stream().filter(a -> a.getPermission() == Permission.READ).count());
    }

    private static QueueData createQueueData(int r, int w, int perm) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(BROKER_NAME);
        queueData.setReadQueueNums(r);
        queueData.setWriteQueueNums(w);
        queueData.setPerm(perm);
        return queueData;
    }
}
