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
//package org.apache.rocketmq.proxy.grpc.v2.service;
//
//import apache.rocketmq.v2.Endpoints;
//import apache.rocketmq.v2.QueryAssignmentRequest;
//import apache.rocketmq.v2.QueryAssignmentResponse;
//import apache.rocketmq.v2.QueryRouteRequest;
//import apache.rocketmq.v2.QueryRouteResponse;
//import io.grpc.Context;
//import java.util.concurrent.CompletableFuture;
//import org.apache.rocketmq.proxy.common.ParameterConverter;
//import org.apache.rocketmq.proxy.service.ServiceManager;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.AssignmentQueueSelector;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.DefaultAssignmentQueueSelector;
//
//public abstract class AbstractRouteService extends BaseService {
//    protected volatile ParameterConverter<Endpoints, Endpoints> queryRouteEndpointConverter;
//    protected volatile ResponseHook<QueryRouteRequest, QueryRouteResponse> queryRouteHook;
//
//    protected volatile ParameterConverter<Endpoints, Endpoints> queryAssignmentEndpointConverter;
//    protected volatile ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignmentHook;
//    protected volatile AssignmentQueueSelector assignmentQueueSelector;
//
//    protected final GrpcClientManager grpcClientManager;
//
//    public AbstractRouteService(ServiceManager serviceManager, GrpcClientManager grpcClientManager) {
//        super(serviceManager);
//        this.grpcClientManager = grpcClientManager;
//        this.queryRouteEndpointConverter = (ctx, parameter) -> parameter;
//        this.queryAssignmentEndpointConverter = (ctx, parameter) -> parameter;
//        this.assignmentQueueSelector = new DefaultAssignmentQueueSelector(this.serviceManager.getTopicRouteService());
//    }
//
//    public abstract CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request);
//
//    public abstract CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request);
//
//    public ParameterConverter<Endpoints, Endpoints> getQueryRouteEndpointConverter() {
//        return queryRouteEndpointConverter;
//    }
//
//    public void setQueryRouteEndpointConverter(
//        ParameterConverter<Endpoints, Endpoints> queryRouteEndpointConverter) {
//        this.queryRouteEndpointConverter = queryRouteEndpointConverter;
//    }
//
//    public ResponseHook<QueryRouteRequest, QueryRouteResponse> getQueryRouteHook() {
//        return queryRouteHook;
//    }
//
//    public void setQueryRouteHook(
//        ResponseHook<QueryRouteRequest, QueryRouteResponse> queryRouteHook) {
//        this.queryRouteHook = queryRouteHook;
//    }
//
//    public ParameterConverter<Endpoints, Endpoints> getQueryAssignmentEndpointConverter() {
//        return queryAssignmentEndpointConverter;
//    }
//
//    public void setQueryAssignmentEndpointConverter(
//        ParameterConverter<Endpoints, Endpoints> queryAssignmentEndpointConverter) {
//        this.queryAssignmentEndpointConverter = queryAssignmentEndpointConverter;
//    }
//
//    public AssignmentQueueSelector getAssignmentQueueSelector() {
//        return assignmentQueueSelector;
//    }
//
//    public void setAssignmentQueueSelector(
//        AssignmentQueueSelector assignmentQueueSelector) {
//        this.assignmentQueueSelector = assignmentQueueSelector;
//    }
//
//    public ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> getQueryAssignmentHook() {
//        return queryAssignmentHook;
//    }
//
//    public void setQueryAssignmentHook(
//        ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignmentHook) {
//        this.queryAssignmentHook = queryAssignmentHook;
//    }
//}
