///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.rocketmq.proxy.grpc.v2.service.local;
//
//import apache.rocketmq.v2.Address;
//import apache.rocketmq.v2.AddressScheme;
//import apache.rocketmq.v2.Assignment;
//import apache.rocketmq.v2.Broker;
//import apache.rocketmq.v2.Code;
//import apache.rocketmq.v2.Endpoints;
//import apache.rocketmq.v2.MessageQueue;
//import apache.rocketmq.v2.Permission;
//import apache.rocketmq.v2.QueryAssignmentRequest;
//import apache.rocketmq.v2.QueryAssignmentResponse;
//import apache.rocketmq.v2.QueryRouteRequest;
//import apache.rocketmq.v2.QueryRouteResponse;
//import com.google.common.net.HostAndPort;
//import io.grpc.Context;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import org.apache.rocketmq.common.protocol.route.BrokerData;
//import org.apache.rocketmq.common.protocol.route.QueueData;
//import org.apache.rocketmq.common.protocol.route.TopicRouteData;
//import org.apache.rocketmq.proxy.config.ConfigurationManager;
//import org.apache.rocketmq.proxy.service.ServiceManager;
//import org.apache.rocketmq.proxy.service.route.MessageQueueView;
//import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
//import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
//import org.apache.rocketmq.proxy.grpc.v2.service.AbstractRouteService;
//import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
//
//public class RouteService extends AbstractRouteService {
//    public RouteService(ServiceManager serviceManager, GrpcClientManager grpcClientManager) {
//        super(serviceManager, grpcClientManager);
//    }
//
//    @Override
//    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
//        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
//        future.whenComplete((response, throwable) -> {
//            if (queryRouteHook != null) {
//                queryRouteHook.beforeResponse(ctx, request, response, throwable);
//            }
//        });
//
//        try {
//            String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
//            MessageQueueView messageQueueView = this.serviceManager.getTopicRouteService().getAllMessageQueueView(topicName);
//            TopicRouteData topicRouteData = messageQueueView.getTopicRouteData();
//            List<QueueData> queueDataList = topicRouteData.getQueueDatas();
//            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
//
//            List<MessageQueue> messageQueueList = new ArrayList<>();
//            Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(brokerDataList);
//
//            for (QueueData queueData : queueDataList) {
//                String brokerName = queueData.getBrokerName();
//                Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
//                if (brokerIdMap == null) {
//                    break;
//                }
//                for (Broker broker : brokerIdMap.values()) {
//                    messageQueueList.addAll(GrpcConverter.genMessageQueueFromQueueData(queueData, request.getTopic(), broker));
//                }
//            }
//
//            QueryRouteResponse response = QueryRouteResponse.newBuilder()
//                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
//                .addAllMessageQueues(messageQueueList)
//                .build();
//            future.complete(response);
//        } catch (Throwable t) {
//            if (TopicRouteHelper.isTopicNotExistError(t)) {
//                future.complete(QueryRouteResponse.newBuilder()
//                    .setStatus(ResponseBuilder.buildStatus(Code.TOPIC_NOT_FOUND, t.getMessage()))
//                    .build());
//            } else {
//                future.completeExceptionally(t);
//            }
//        }
//        return future;
//    }
//
//    @Override
//    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
//        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();
//        future.whenComplete((response, throwable) -> {
//            if (queryAssignmentHook != null) {
//                queryAssignmentHook.beforeResponse(ctx, request, response, throwable);
//            }
//        });
//
//        try {
//            List<Assignment> assignments = new ArrayList<>();
//            List<SelectableMessageQueue> messageQueueList = this.assignmentQueueSelector.getAssignment(ctx, request);
//            String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
//            MessageQueueView messageQueueView = this.serviceManager.getTopicRouteService().getAllMessageQueueView(topicName);
//            TopicRouteData topicRouteData = messageQueueView.getTopicRouteData();
//            Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(topicRouteData.getBrokerDatas());
//            for (SelectableMessageQueue messageQueue : messageQueueList) {
//                Map<Long, Broker> brokerIdMap = brokerMap.get(messageQueue.getBrokerName());
//                if (brokerIdMap != null) {
//                    Broker broker = brokerIdMap.get(0L);
//
//                    MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
//                        .setTopic(request.getTopic())
//                        .setId(-1)
//                        .setPermission(Permission.READ_WRITE)
//                        .setBroker(broker)
//                        .build();
//
//                    assignments.add(Assignment.newBuilder()
//                        .setMessageQueue(defaultMessageQueue)
//                        .build());
//                }
//            }
//            QueryAssignmentResponse response = QueryAssignmentResponse.newBuilder()
//                .addAllAssignments(assignments)
//                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
//                .build();
//            future.complete(response);
//        } catch (Throwable t) {
//            future.completeExceptionally(t);
//        }
//        return future;
//    }
//
//    private Map<String/*brokerName*/, Map<Long/*brokerID*/, Broker>> buildBrokerMap(List<BrokerData> brokerDataList) {
//        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();
//        for (BrokerData brokerData : brokerDataList) {
//            Map<Long, Broker> brokerIdMap = new HashMap<>();
//            String brokerName = brokerData.getBrokerName();
//            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
//                Long brokerId = entry.getKey();
//                HostAndPort hostAndPort = HostAndPort.fromString(entry.getValue());
//                Broker broker = Broker.newBuilder()
//                    .setName(brokerName)
//                    .setId(Math.toIntExact(brokerId))
//                    .setEndpoints(Endpoints.newBuilder()
//                        .setScheme(AddressScheme.IPv4)
//                        .addAddresses(
//                            Address.newBuilder()
//                                .setPort(ConfigurationManager.getProxyConfig().getGrpcServerPort())
//                                .setHost(hostAndPort.getHost())
//                        )
//                        .build())
//                    .build();
//
//                brokerIdMap.put(brokerId, broker);
//            }
//            brokerMap.put(brokerName, brokerIdMap);
//        }
//        return brokerMap;
//    }
//}
