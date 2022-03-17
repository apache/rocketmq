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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.*;
import com.google.rpc.Code;
import io.grpc.Context;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteHelper;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ParameterConverter;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RouteService extends BaseService {

    private volatile ParameterConverter<Endpoints, Endpoints> queryRouteEndpointConverter;
    private volatile ResponseHook<QueryRouteRequest, QueryRouteResponse> queryRouteHook = null;

    private volatile ParameterConverter<Endpoints, Endpoints> queryAssignmentEndpointConverter;
    private volatile RouteAssignmentQueueSelector assignmentQueueSelector;
    private volatile ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignmentHook = null;

    public RouteService(ConnectorManager connectorManager) {
        super(connectorManager);

        queryRouteEndpointConverter = (ctx, parameter) -> parameter;
        queryAssignmentEndpointConverter = (ctx, parameter) -> parameter;
        assignmentQueueSelector = new DefaultRouteAssignmentQueueSelector(this.connectorManager.getTopicRouteCache());
    }

    public void setQueryRouteEndpointConverter(ParameterConverter<Endpoints, Endpoints> queryRouteEndpointConverter) {
        this.queryRouteEndpointConverter = queryRouteEndpointConverter;
    }

    public void setQueryRouteHook(ResponseHook<QueryRouteRequest, QueryRouteResponse> queryRouteHook) {
        this.queryRouteHook = queryRouteHook;
    }

    public void setQueryAssignmentEndpointConverter(
        ParameterConverter<Endpoints, Endpoints> queryAssignmentEndpointConverter) {
        this.queryAssignmentEndpointConverter = queryAssignmentEndpointConverter;
    }

    public void setAssignmentQueueSelector(RouteAssignmentQueueSelector assignmentQueueSelector) {
        this.assignmentQueueSelector = assignmentQueueSelector;
    }

    public void setQueryAssignmentHook(
        ResponseHook<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignmentHook) {
        this.queryAssignmentHook = queryAssignmentHook;
    }

    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (queryRouteHook != null) {
                queryRouteHook.beforeResponse(request, response, throwable);
            }
        });

        try {
            Endpoints resEndpoints = this.queryRouteEndpointConverter.convert(ctx, request.getEndpoints());
            if (resEndpoints == null || resEndpoints.getDefaultInstanceForType().equals(resEndpoints)) {
                future.complete(QueryRouteResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "endpoint " +
                        request.getEndpoints() + " is invalidate"))
                    .build());
                return future;
            }

            MessageQueueWrapper messageQueueWrapper = this.connectorManager.getTopicRouteCache()
                .getMessageQueue(Converter.getResourceNameWithNamespace(request.getTopic()));
            TopicRouteData topicRouteData = messageQueueWrapper.getTopicRouteData();
            List<QueueData> queueDataList = topicRouteData.getQueueDatas();

            List<Partition> partitionList = new ArrayList<>();
            for (QueueData queueData : queueDataList) {
                Broker broker = Broker.newBuilder()
                    .setName(queueData.getBrokerName())
                    .setId(0)
                    .setEndpoints(resEndpoints)
                    .build();

                partitionList.addAll(genPartitionFromQueueData(queueData, request.getTopic(), broker));
            }
            QueryRouteResponse response = QueryRouteResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
                .addAllPartitions(partitionList)
                .build();
            future.complete(response);
        } catch (Throwable t) {
            if (TopicRouteHelper.isTopicNotExistError(t)) {
                future.complete(QueryRouteResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.NOT_FOUND, t.getMessage()))
                    .build());
            } else {
                future.completeExceptionally(t);
            }
        }
        return future;
    }

    protected static List<Partition> genPartitionFromQueueData(QueueData queueData, Resource topic, Broker broker) {
        List<Partition> partitionList = new ArrayList<>();

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

        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means readable and writable queue nums.
        int queueIdIndex = 0;
        for(int i = 0; i < r; i++){
            Partition partition = buildPartition(broker, topic, queueIdIndex++, Permission.READ);
            partitionList.add(partition);
        }

        for(int i = 0; i < w; i++){
            Partition partition = buildPartition(broker, topic, queueIdIndex++, Permission.WRITE);
            partitionList.add(partition);
        }

        for (int i = 0; i < rw; i++) {
            Partition partition = buildPartition(broker, topic, queueIdIndex++, Permission.READ_WRITE);
            partitionList.add(partition);
        }

        return partitionList;
    }

    private static Partition buildPartition(Broker broker, Resource topic, int queueId, Permission perm) {
        Partition.Builder builder = Partition.newBuilder()
                .setBroker(broker)
                .setTopic(topic)
                .setId(queueId);
        builder.setPermission(perm);
        return builder.build();
    }

    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (queryAssignmentHook != null) {
                queryAssignmentHook.beforeResponse(request, response, throwable);
            }
        });

        try {
            Endpoints resEndpoints = this.queryAssignmentEndpointConverter.convert(ctx, request.getEndpoints());
            if (resEndpoints == null || Endpoints.getDefaultInstance().equals(resEndpoints)) {
                future.complete(QueryAssignmentResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "endpoint " +
                        request.getEndpoints() + " is invalidate"))
                    .build());
                return future;
            }

            List<Assignment> assignments = new ArrayList<>();
            List<SelectableMessageQueue> messageQueueList = this.assignmentQueueSelector.getAssignment(ctx, request);

            for (SelectableMessageQueue messageQueue : messageQueueList) {
                Broker broker = Broker.newBuilder()
                    .setName(messageQueue.getBrokerName())
                    .setId(0)
                    .setEndpoints(resEndpoints)
                    .build();

                Partition defaultPartition = Partition.newBuilder()
                    .setTopic(request.getTopic())
                    .setId(-1)
                    .setPermission(Permission.READ_WRITE)
                    .setBroker(broker)
                    .build();

                assignments.add(Assignment.newBuilder()
                    .setPartition(defaultPartition)
                    .build());
            }
            QueryAssignmentResponse response = QueryAssignmentResponse.newBuilder()
                .addAllAssignments(assignments)
                .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
                .build();
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
