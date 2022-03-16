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

package org.apache.rocketmq.proxy.grpc.service;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.ChangeInvisibleDurationRequest;
import apache.rocketmq.v1.ChangeInvisibleDurationResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.NoopCommand;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.NotifyClientTerminationResponse;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.ReportMessageConsumptionResultRequest;
import apache.rocketmq.v1.ReportMessageConsumptionResultResponse;
import apache.rocketmq.v1.ReportThreadStackTraceRequest;
import apache.rocketmq.v1.ReportThreadStackTraceResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.service.cluster.ClientService;
import org.apache.rocketmq.proxy.grpc.service.cluster.ReceiveMessageService;
import org.apache.rocketmq.proxy.grpc.service.cluster.ProducerService;
import org.apache.rocketmq.proxy.grpc.service.cluster.RouteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterGrpcService extends AbstractStartAndShutdown implements GrpcForwardService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("ClusterGrpcServiceScheduledThread"));

    private final ChannelManager channelManager;

    private final ConnectorManager connectorManager;
    private final ProducerService producerService;
    private final ReceiveMessageService receiveMessageService;
    private final RouteService routeService;
    private final ClientService clientService;

    public ClusterGrpcService() {
        this.channelManager = new ChannelManager();
        this.connectorManager = new ConnectorManager(checkData -> {
        });
        this.receiveMessageService = new ReceiveMessageService(connectorManager);
        this.producerService = new ProducerService(connectorManager);
        this.routeService = new RouteService(connectorManager);
        this.clientService = new ClientService(scheduledExecutorService);

        this.appendStartAndShutdown(new ClusterGrpcServiceStartAndShutdown());
        this.appendStartAndShutdown(this.connectorManager);
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        return this.routeService.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        this.clientService.heartbeat(ctx, request, channelManager);
        return CompletableFuture.completedFuture(HeartbeatResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .build());
    }

    @Override
    public CompletableFuture<HealthCheckResponse> healthCheck(Context ctx, HealthCheckRequest request) {
        final HealthCheckResponse response = HealthCheckResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .build();
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        return this.producerService.sendMessage(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        return this.routeService.queryAssignment(ctx, request);
    }

    @Override
    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        return this.receiveMessageService.receiveMessage(ctx, request);
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        return null;
    }

    @Override public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        return null;
    }

    @Override public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        return null;
    }

    @Override public CompletableFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<PollCommandResponse> pollCommand(Context ctx, PollCommandRequest request) {
        CompletableFuture<PollCommandResponse> future = new CompletableFuture<>();
        String clientId = request.getClientId();
        PollCommandResponse noopCommandResponse = PollCommandResponse.newBuilder().setNoopCommand(NoopCommand.newBuilder().build()).build();

        switch (request.getGroupCase()) {
            case PRODUCER_GROUP:
                Resource producerGroup = request.getProducerGroup();
                String producerGroupName = Converter.getResourceNameWithNamespace(producerGroup);
                GrpcClientChannel producerChannel = GrpcClientChannel.getChannel(this.channelManager, producerGroupName, clientId);
                if (producerChannel == null) {
                    future.complete(noopCommandResponse);
                } else {
                    producerChannel.addClientObserver(future);
                }
                break;
            case CONSUMER_GROUP:
                Resource consumerGroup = request.getConsumerGroup();
                String consumerGroupName = Converter.getResourceNameWithNamespace(consumerGroup);
                GrpcClientChannel consumerChannel = GrpcClientChannel.getChannel(this.channelManager, consumerGroupName, clientId);
                if (consumerChannel == null) {
                    future.complete(noopCommandResponse);
                } else {
                    consumerChannel.addClientObserver(future);
                }
                break;
            default:
                break;
        }
        return future;
    }

    @Override public CompletableFuture<ReportThreadStackTraceResponse> reportThreadStackTrace(Context ctx,
        ReportThreadStackTraceRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ReportMessageConsumptionResultResponse> reportMessageConsumptionResult(Context ctx,
        ReportMessageConsumptionResultRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
        NotifyClientTerminationRequest request) {
        this.clientService.unregister(ctx, request, channelManager);
        return CompletableFuture.completedFuture(NotifyClientTerminationResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .build());
    }

    @Override public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        return null;
    }

    private class ClusterGrpcServiceStartAndShutdown implements StartAndShutdown {

        @Override
        public void start() throws Exception {

        }

        @Override
        public void shutdown() throws Exception {
            scheduledExecutorService.shutdown();
        }
    }
}
