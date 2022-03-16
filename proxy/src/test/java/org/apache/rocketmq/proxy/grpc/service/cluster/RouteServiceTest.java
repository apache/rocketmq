package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.Resource;
import com.google.rpc.Code;
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
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

public class RouteServiceTest extends BaseServiceTest {

    @Override
    public void beforeEach() throws Throwable {
        TopicRouteData routeData = new TopicRouteData();

        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("cluster");
        brokerData.setBrokerName("brokerName");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>() {{
            put(0L, "127.0.0.1:10911");
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
    public void testQueryRouteWithInvalidEndpoints() {
        RouteService routeService = new RouteService(this.clientManager);

        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .build());

        try {
            QueryRouteResponse response = future.get();
            assertEquals(Code.INVALID_ARGUMENT.getNumber(), response.getCommon().getStatus().getCode());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testQueryRoute() {
        RouteService routeService = new RouteService(this.clientManager);

        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .setEndpoints(Endpoints.newBuilder()
                .addAddresses(Address.newBuilder()
                    .setPort(80)
                    .setHost("host")
                    .build())
                .setScheme(AddressScheme.DOMAIN_NAME)
                .build())
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .build());

        try {
            QueryRouteResponse response = future.get();
            assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
            assertEquals(8, response.getPartitionsCount());
            assertEquals("host", response.getPartitions(0).getBroker()
                .getEndpoints().getAddresses(0).getHost());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testQueryRouteWhenTopicNotExist() {
        RouteService routeService = new RouteService(this.clientManager);

        CompletableFuture<QueryRouteResponse> future = routeService.queryRoute(Context.current(), QueryRouteRequest.newBuilder()
            .setEndpoints(Endpoints.newBuilder()
                .addAddresses(Address.newBuilder()
                    .setPort(80)
                    .setHost("host")
                    .build())
                .setScheme(AddressScheme.DOMAIN_NAME)
                .build())
            .setTopic(Resource.newBuilder()
                .setName("notExistTopic")
                .build())
            .build());

        try {
            QueryRouteResponse response = future.get();
            assertEquals(Code.NOT_FOUND.getNumber(), response.getCommon().getStatus().getCode());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testQueryAssignmentInvalidEndpoints() {
        RouteService routeService = new RouteService(this.clientManager);

        CompletableFuture<QueryAssignmentResponse> future = routeService.queryAssignment(Context.current(), QueryAssignmentRequest.newBuilder()
            .build());

        try {
            QueryAssignmentResponse response = future.get();
            assertEquals(Code.INVALID_ARGUMENT.getNumber(), response.getCommon().getStatus().getCode());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testQueryAssignment() {
        RouteService routeService = new RouteService(this.clientManager);

        CompletableFuture<QueryAssignmentResponse> future = routeService.queryAssignment(Context.current(), QueryAssignmentRequest.newBuilder()
            .setEndpoints(Endpoints.newBuilder()
                .addAddresses(Address.newBuilder()
                    .setPort(80)
                    .setHost("host")
                    .build())
                .setScheme(AddressScheme.DOMAIN_NAME)
                .build())
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setClientId("clientId")
            .build());

        try {
            QueryAssignmentResponse response = future.get();
            assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
            assertEquals(1, response.getAssignmentsCount());
            assertEquals("brokerName", response.getAssignments(0).getPartition().getBroker().getName());
            assertEquals("host", response.getAssignments(0).getPartition().getBroker().getEndpoints().getAddresses(0).getHost());
        } catch (Exception e) {
            assertNull(e);
        }
    }
}