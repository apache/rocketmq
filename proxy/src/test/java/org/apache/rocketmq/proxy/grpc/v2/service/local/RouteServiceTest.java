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

package org.apache.rocketmq.proxy.grpc.v2.service.local;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
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
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.connector.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.BaseServiceTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class RouteServiceTest extends BaseServiceTest {
    private String brokerAddress = "127.0.0.1:10911";
    private static final Settings WITH_HOST_SETTINGS = Settings.newBuilder()
        .setAccessPoint(Endpoints.newBuilder()
            .addAddresses(Address.newBuilder()
                .setPort(80)
                .setHost("host")
                .build())
            .setScheme(AddressScheme.DOMAIN_NAME)
            .build())
        .build();

    @Test
    public void testLocalModeQueryRoute() throws Exception {
        RouteService routeService = new RouteService(this.connectorManager, this.grpcClientManager);
        routeService.start();

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
    public void testLocalModeQueryAssignment() throws Exception {
        RouteService routeService = new RouteService(this.connectorManager, this.grpcClientManager);
        routeService.start();

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

    @Override public void beforeEach() throws Throwable {
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
}