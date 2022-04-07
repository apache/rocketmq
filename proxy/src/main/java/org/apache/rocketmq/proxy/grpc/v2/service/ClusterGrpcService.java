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

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.PullMessageResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateCheckRequest;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.common.PollResponseManager;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyMode;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.ConsumerService;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.ForwardClientService;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.ProducerService;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.PullMessageService;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.RouteService;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.TransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterGrpcService extends AbstractStartAndShutdown implements GrpcForwardService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("ClusterGrpcServiceScheduledThread"));

    private final ChannelManager channelManager;
    private final ConnectorManager connectorManager;
    private final ProducerService producerService;
    private final ConsumerService consumerService;
    private final RouteService routeService;
    private final ForwardClientService clientService;
    private final PullMessageService pullMessageService;
    private final TransactionService transactionService;
    private final PollResponseManager pollCommandResponseManager;

    public ClusterGrpcService() {
        this.channelManager = new ChannelManager();
        this.pollCommandResponseManager = new PollResponseManager();
        this.connectorManager = new ConnectorManager(new GrpcTransactionStateChecker());
        this.consumerService = new ConsumerService(connectorManager);
        this.producerService = new ProducerService(connectorManager);
        this.routeService = new RouteService(ProxyMode.CLUSTER, connectorManager);
        this.clientService = new ForwardClientService(connectorManager, scheduledExecutorService, channelManager, pollCommandResponseManager);
        this.pullMessageService = new PullMessageService(connectorManager);
        this.transactionService = new TransactionService(connectorManager, channelManager);

        this.appendStartAndShutdown(new ClusterGrpcServiceStartAndShutdown());
        this.appendStartAndShutdown(this.connectorManager);
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        return routeService.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        return producerService.sendMessage(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        return routeService.queryAssignment(ctx, request);
    }

    @Override
    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        return consumerService.receiveMessage(ctx, request);
    }

    @Override
    public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        return consumerService.nackMessage(ctx, request);
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        return consumerService.ackMessage(ctx, request);
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return producerService.forwardMessageToDeadLetterQueue(ctx, request);
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        return transactionService.endTransaction(ctx, request);
    }

    @Override
    public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        return pullMessageService.queryOffset(ctx, request);
    }

    @Override
    public CompletableFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        return pullMessageService.pullMessage(ctx, request);
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
        NotifyClientTerminationRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        return null;
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Context ctx, StreamObserver<TelemetryCommand> responseObserver) {
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

    private class GrpcTransactionStateChecker implements TransactionStateChecker {

        @Override
        public void checkTransactionState(TransactionStateCheckRequest checkData) {
            transactionService.checkTransactionState(checkData);
        }
    }
}
