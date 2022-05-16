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
//import apache.rocketmq.v2.Resource;
//import apache.rocketmq.v2.Settings;
//import apache.rocketmq.v2.TelemetryCommand;
//import io.grpc.Context;
//import io.grpc.stub.StreamObserver;
//import org.apache.rocketmq.proxy.grpc.v2.common.ChannelManager;
//import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
//import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
//
//public class ClientSettingsService {
//
//    private final ChannelManager channelManager;
//    private final GrpcClientManager grpcClientManager;
//    private final TelemetryCommandManager telemetryCommandManager;
//
//    public ClientSettingsService(ChannelManager channelManager,
//        GrpcClientManager grpcClientManager,
//        TelemetryCommandManager telemetryCommandManager) {
//        this.channelManager = channelManager;
//        this.grpcClientManager = grpcClientManager;
//        this.telemetryCommandManager = telemetryCommandManager;
//    }
//
//    public TelemetryCommand processClientSettings(Context ctx, TelemetryCommand request, StreamObserver<TelemetryCommand> responseObserver) {
//        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
//        grpcClientManager.updateClientSettings(clientId, request.getSettings());
//        Settings settings = grpcClientManager.getClientSettings(clientId);
//        if (settings.hasPublishing()) {
//            for (Resource topic : settings.getPublishing().getTopicsList()) {
//                String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
//                GrpcClientChannel producerChannel = GrpcClientChannel.create(channelManager, topicName, clientId, telemetryCommandManager);
//                producerChannel.setClientObserver(responseObserver);
//            }
//        }
//        if (settings.hasSubscription()) {
//            String groupName = GrpcConverter.wrapResourceWithNamespace(settings.getSubscription().getGroup());
//            GrpcClientChannel consumerChannel = GrpcClientChannel.create(channelManager, groupName, clientId, telemetryCommandManager);
//            consumerChannel.setClientObserver(responseObserver);
//        }
//        return TelemetryCommand.newBuilder()
//            .setSettings(settings)
//            .build();
//    }
//}
