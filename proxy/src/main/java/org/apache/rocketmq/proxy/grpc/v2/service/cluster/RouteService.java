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
import apache.rocketmq.v2.Assignment;
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
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.common.ParameterConverter;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteHelper;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyMode;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;

public class RouteService extends BaseService {
    private final ProxyMode mode;

    private volatile ParameterConverter<Endpoints, Endpoints> queryRouteEndpointConverter;
    private volatile ResponseHook<QueryRouteRequest, QueryRouteResponse> queryRouteHook;

    private volatile ParameterConverter<Endpoints, Endpoints> queryAssignmentEndpointConverter;
    private volatile AssignmentQueueSelector assignmentQueueSelector;
    private volatile ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignmentHook;

    private GrpcClientManager grpcClientManager;

    public RouteService(ProxyMode mode, ConnectorManager connectorManager, GrpcClientManager grpcClientManager) {
        super(connectorManager);
        Preconditions.checkArgument(ProxyMode.isClusterMode(mode) || ProxyMode.isLocalMode(mode));
        this.mode = mode;
        queryRouteEndpointConverter = (ctx, parameter) -> parameter;
        queryAssignmentEndpointConverter = (ctx, parameter) -> parameter;
        assignmentQueueSelector = new DefaultAssignmentQueueSelector(this.connectorManager.getTopicRouteCache());
        this.grpcClientManager = grpcClientManager;
    }

    public void setQueryRouteEndpointConverter(ParameterConverter<Endpoints, Endpoints> queryRouteEndpointConverter) {
        this.queryRouteEndpointConverter = queryRouteEndpointConverter;
    }

    public void setQueryRouteHook(ResponseHook<QueryRouteRequest, QueryRouteResponse> queryRouteHook) {
        this.queryRouteHook = queryRouteHook;
    }

    public void setQueryAssignmentEndpointConverter(ParameterConverter<Endpoints, Endpoints> queryAssignmentEndpointConverter) {
        this.queryAssignmentEndpointConverter = queryAssignmentEndpointConverter;
    }

    public void setAssignmentQueueSelector(AssignmentQueueSelector assignmentQueueSelector) {
        this.assignmentQueueSelector = assignmentQueueSelector;
    }

    public void setQueryAssignmentHook(ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignmentHook) {
        this.queryAssignmentHook = queryAssignmentHook;
    }

    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (queryRouteHook != null) {
                queryRouteHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
            MessageQueueWrapper messageQueueWrapper = this.connectorManager.getTopicRouteCache().getMessageQueue(topicName);
            TopicRouteData topicRouteData = messageQueueWrapper.getTopicRouteData();
            List<QueueData> queueDataList = topicRouteData.getQueueDatas();
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();

            List<MessageQueue> messageQueueList = new ArrayList<>();
            if (ProxyMode.isClusterMode(mode.name())) {
                Settings clientSettings = grpcClientManager.getClientSettings(ctx);
                Endpoints resEndpoints = this.queryRouteEndpointConverter.convert(ctx, clientSettings.getAccessPoint());
                if (resEndpoints == null || resEndpoints.getDefaultInstanceForType().equals(resEndpoints)) {
                    future.complete(QueryRouteResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.ILLEGAL_ACCESS_POINT, "endpoint " +
                            clientSettings.getAccessPoint() + " is invalidate"))
                        .build());
                    return future;
                }
                for (QueueData queueData : queueDataList) {
                    Broker broker = Broker.newBuilder()
                        .setName(queueData.getBrokerName())
                        .setId(0)
                        .setEndpoints(resEndpoints)
                        .build();

                    messageQueueList.addAll(genMessageQueueFromQueueData(queueData, request.getTopic(), broker));
                }
            }
            if (ProxyMode.isLocalMode(mode.name())) {
                Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(brokerDataList);

                for (QueueData queueData : queueDataList) {
                    String brokerName = queueData.getBrokerName();
                    Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
                    if (brokerIdMap == null) {
                        break;
                    }
                    for (Broker broker : brokerIdMap.values()) {
                        messageQueueList.addAll(genMessageQueueFromQueueData(queueData, request.getTopic(), broker));
                    }
                }
            }

            QueryRouteResponse response = QueryRouteResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .addAllMessageQueues(messageQueueList)
                .build();
            future.complete(response);
        } catch (Throwable t) {
            if (TopicRouteHelper.isTopicNotExistError(t)) {
                future.complete(QueryRouteResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.TOPIC_NOT_FOUND, t.getMessage()))
                    .build());
            } else {
                future.completeExceptionally(t);
            }
        }
        return future;
    }

    protected static List<MessageQueue> genMessageQueueFromQueueData(QueueData queueData, Resource topic, Broker broker) {
        List<MessageQueue> messageQueueList = new ArrayList<>();

        int r = 0;
        int w = 0;
        int rw = 0;
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            rw = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            r = queueData.getReadQueueNums() - rw;
            w = queueData.getWriteQueueNums() - rw;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            w = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            r = queueData.getReadQueueNums();
        }

        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means both readable and writable queue nums.
        int queueIdIndex = 0;
        for (int i = 0; i < r; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ)
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < w; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.WRITE)
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < rw; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ_WRITE)
                .build();
            messageQueueList.add(messageQueue);
        }

        return messageQueueList;
    }

    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (queryAssignmentHook != null) {
                queryAssignmentHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            List<Assignment> assignments = new ArrayList<>();
            List<SelectableMessageQueue> messageQueueList = this.assignmentQueueSelector.getAssignment(ctx, request);
            if (ProxyMode.isLocalMode(mode)) {
                String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
                MessageQueueWrapper messageQueueWrapper = this.connectorManager.getTopicRouteCache().getMessageQueue(topicName);
                TopicRouteData topicRouteData = messageQueueWrapper.getTopicRouteData();
                Map<String, Map<Long, Broker>> brokerMap = buildBrokerMap(topicRouteData.getBrokerDatas());
                for (SelectableMessageQueue messageQueue : messageQueueList) {
                    Map<Long, Broker> brokerIdMap = brokerMap.get(messageQueue.getBrokerName());
                    if (brokerIdMap != null) {
                        Broker broker = brokerIdMap.get(0L);

                        MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                            .setTopic(request.getTopic())
                            .setId(-1)
                            .setPermission(Permission.READ_WRITE)
                            .setBroker(broker)
                            .build();

                        assignments.add(Assignment.newBuilder()
                            .setMessageQueue(defaultMessageQueue)
                            .build());
                    }
                }
            }
            if (ProxyMode.isClusterMode(mode)) {
                Settings clientSettings = grpcClientManager.getClientSettings(ctx);
                Endpoints resEndpoints = this.queryAssignmentEndpointConverter.convert(ctx, clientSettings.getAccessPoint());
                if (resEndpoints == null || Endpoints.getDefaultInstance().equals(resEndpoints)) {
                    future.complete(QueryAssignmentResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.ILLEGAL_ACCESS_POINT, "endpoint " +
                            clientSettings.getAccessPoint() + " is invalidate"))
                        .build());
                    return future;
                }
                for (SelectableMessageQueue messageQueue : messageQueueList) {
                    Broker broker = Broker.newBuilder()
                        .setName(messageQueue.getBrokerName())
                        .setId(0)
                        .setEndpoints(resEndpoints)
                        .build();

                    MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                        .setTopic(request.getTopic())
                        .setId(-1)
                        .setPermission(Permission.READ_WRITE)
                        .setBroker(broker)
                        .build();

                    assignments.add(Assignment.newBuilder()
                        .setMessageQueue(defaultMessageQueue)
                        .build());
                }
            }

            QueryAssignmentResponse response = QueryAssignmentResponse.newBuilder()
                .addAllAssignments(assignments)
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build();
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private Map<String/*brokerName*/, Map<Long/*brokerID*/, Broker>> buildBrokerMap(List<BrokerData> brokerDataList) {
        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();
        for (BrokerData brokerData : brokerDataList) {
            Map<Long, Broker> brokerIdMap = new HashMap<>();
            String brokerName = brokerData.getBrokerName();
            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                HostAndPort hostAndPort = HostAndPort.fromString(entry.getValue());
                Broker broker = Broker.newBuilder()
                    .setName(brokerName)
                    .setId(Math.toIntExact(brokerId))
                    .setEndpoints(Endpoints.newBuilder()
                        .setScheme(AddressScheme.IPv4)
                        .addAddresses(
                            Address.newBuilder()
                                .setPort(ConfigurationManager.getProxyConfig().getGrpcServerPort())
                                .setHost(hostAndPort.getHost())
                        )
                        .build())
                    .build();

                brokerIdMap.put(brokerId, broker);
            }
            brokerMap.put(brokerName, brokerIdMap);
        }
        return brokerMap;
    }
}
