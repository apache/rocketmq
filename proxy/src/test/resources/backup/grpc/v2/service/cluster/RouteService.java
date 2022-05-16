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
//package org.apache.rocketmq.proxy.grpc.v2.service.cluster;
//
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
//import io.grpc.Context;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import org.apache.rocketmq.common.protocol.route.QueueData;
//import org.apache.rocketmq.common.protocol.route.TopicRouteData;
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
//
//            List<MessageQueue> messageQueueList = new ArrayList<>();
//            Endpoints endpoints = request.getEndpoints();
//            Endpoints resEndpoints = this.queryRouteEndpointConverter.convert(ctx, endpoints);
//            if (resEndpoints == null || resEndpoints.getDefaultInstanceForType().equals(resEndpoints)) {
//                future.complete(QueryRouteResponse.newBuilder()
//                    .setStatus(ResponseBuilder.buildStatus(Code.ILLEGAL_ACCESS_POINT, "endpoint " +
//                        endpoints + " is invalidate"))
//                    .build());
//                return future;
//            }
//            for (QueueData queueData : queueDataList) {
//                Broker broker = Broker.newBuilder()
//                    .setName(queueData.getBrokerName())
//                    .setId(0)
//                    .setEndpoints(resEndpoints)
//                    .build();
//
//                messageQueueList.addAll(GrpcConverter.genMessageQueueFromQueueData(queueData, request.getTopic(), broker));
//            }
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
//            Endpoints endpoints = request.getEndpoints();
//            Endpoints resEndpoints = this.queryAssignmentEndpointConverter.convert(ctx, endpoints);
//            if (resEndpoints == null || Endpoints.getDefaultInstance().equals(resEndpoints)) {
//                future.complete(QueryAssignmentResponse.newBuilder()
//                    .setStatus(ResponseBuilder.buildStatus(Code.ILLEGAL_ACCESS_POINT, "endpoint " +
//                        endpoints + " is invalidate"))
//                    .build());
//                return future;
//            }
//            for (SelectableMessageQueue messageQueue : messageQueueList) {
//                Broker broker = Broker.newBuilder()
//                    .setName(messageQueue.getBrokerName())
//                    .setId(0)
//                    .setEndpoints(resEndpoints)
//                    .build();
//
//                MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
//                    .setTopic(request.getTopic())
//                    .setId(-1)
//                    .setPermission(Permission.READ_WRITE)
//                    .setBroker(broker)
//                    .build();
//
//                assignments.add(Assignment.newBuilder()
//                    .setMessageQueue(defaultMessageQueue)
//                    .build());
//            }
//
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
//}
