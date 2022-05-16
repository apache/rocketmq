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
//import apache.rocketmq.v2.QueryAssignmentRequest;
//import io.grpc.Context;
//import java.util.List;
//import org.apache.rocketmq.proxy.service.route.MessageQueueView;
//import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
//import org.apache.rocketmq.proxy.service.route.TopicRouteService;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
//
//public class DefaultAssignmentQueueSelector implements AssignmentQueueSelector {
//
//    private final TopicRouteService topicRouteService;
//
//    public DefaultAssignmentQueueSelector(TopicRouteService topicRouteService) {
//        this.topicRouteService = topicRouteService;
//    }
//
//    @Override
//    public List<SelectableMessageQueue> getAssignment(Context ctx, QueryAssignmentRequest request) throws Exception {
//        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
//        MessageQueueView messageQueueView = topicRouteService.getAllMessageQueueView(topicName);
//        return messageQueueView.getReadSelector().getBrokerActingQueues();
//    }
//}