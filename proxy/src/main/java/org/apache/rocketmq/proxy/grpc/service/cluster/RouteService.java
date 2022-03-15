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

import apache.rocketmq.v1.Assignment;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.Resource;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.proxy.client.ForwardClientManager;
import org.apache.rocketmq.proxy.client.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.client.route.MessageQueueWrapper;
import org.apache.rocketmq.proxy.common.RocketMQHelper;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;

public class RouteService extends BaseService {

    private volatile RouteAssignmentQueueSelector assignmentQueueSelector = new DefaultRouteAssignmentQueueSelector();
    private volatile QueryRouteHook queryRouteHook = null;
    private volatile QueryAssignmentHook queryAssignmentHook = null;

    public RouteService(ForwardClientManager clientManager) {
        super(clientManager);
    }

    public interface QueryRouteHook {
        QueryRouteResponse beforeResponse(Context ctx, QueryRouteRequest request, QueryRouteResponse response);
    }

    public interface QueryAssignmentHook {
        QueryAssignmentResponse beforeResponse(Context ctx, QueryAssignmentRequest request,
            QueryAssignmentResponse response);
    }

    public interface RouteAssignmentQueueSelector {
        List<SelectableMessageQueue> getAssignment(QueryAssignmentRequest request) throws Exception;
    }

    public class DefaultRouteAssignmentQueueSelector implements RouteAssignmentQueueSelector {

        @Override
        public List<SelectableMessageQueue> getAssignment(QueryAssignmentRequest request) throws Exception {
            MessageQueueWrapper messageQueueWrapper = clientManager.getTopicRouteCache()
                .getMessageQueue(Converter.getResourceNameWithNamespace(request.getTopic()));
            return messageQueueWrapper.getReadSelector().getBrokerActingQueues();
        }
    }

    public void setQueryRouteHook(QueryRouteHook queryRouteHook) {
        this.queryRouteHook = queryRouteHook;
    }

    public void setAssignmentQueueSelector(RouteAssignmentQueueSelector assignmentQueueSelector) {
        this.assignmentQueueSelector = assignmentQueueSelector;
    }

    public void setQueryAssignmentHook(QueryAssignmentHook queryAssignmentHook) {
        this.queryAssignmentHook = queryAssignmentHook;
    }

    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        CompletableFuture<QueryRouteResponse> resFuture = future.thenApply(r -> {
            if (this.queryRouteHook != null) {
                return this.queryRouteHook.beforeResponse(ctx, request, r);
            }
            return r;
        });

        try {
            Endpoints resEndpoints = request.getEndpoints();
            if (resEndpoints.getDefaultInstanceForType().equals(resEndpoints)) {
                future.complete(QueryRouteResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "endpoint " +
                        request.getEndpoints() + " is invalidate"))
                    .build());
                return resFuture;
            }

            MessageQueueWrapper messageQueueWrapper = this.clientManager.getTopicRouteCache()
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
            if (RocketMQHelper.isTopicNotExistError(t)) {
                future.complete(QueryRouteResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.NOT_FOUND, t.getMessage()))
                    .build());
            } else {
                future.completeExceptionally(t);
            }
        }
        return resFuture;
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

        for (int i = 0; i < (rw + r + w); i++) {
            Partition.Builder builder = Partition.newBuilder()
                .setBroker(broker)
                .setTopic(topic)
                .setId(i);
            if (i < r) {
                builder.setPermission(Permission.READ);
            } else if (i < w) {
                builder.setPermission(Permission.WRITE);
            } else {
                builder.setPermission(Permission.READ_WRITE);
            }
            partitionList.add(builder.build());
        }
        return partitionList;
    }

    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();
        CompletableFuture<QueryAssignmentResponse> resFuture = future.thenApply(r -> {
            if (this.queryAssignmentHook != null) {
                return this.queryAssignmentHook.beforeResponse(ctx, request, r);
            }
            return r;
        });
        try {
            Endpoints resEndpoints = request.getEndpoints();
            if (resEndpoints.getDefaultInstanceForType().equals(resEndpoints)) {
                future.complete(QueryAssignmentResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "endpoint " +
                        request.getEndpoints() + " is invalidate"))
                    .build());
                return resFuture;
            }

            List<Assignment> assignments = new ArrayList<>();
            List<SelectableMessageQueue> messageQueueList = this.assignmentQueueSelector.getAssignment(request);

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
            if (this.queryAssignmentHook != null) {
                this.queryAssignmentHook.beforeResponse(ctx, request, response);
            }
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return resFuture;
    }
}
