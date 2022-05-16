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
//import apache.rocketmq.v2.AckMessageRequest;
//import apache.rocketmq.v2.AckMessageResponse;
//import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
//import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
//import apache.rocketmq.v2.EndTransactionRequest;
//import apache.rocketmq.v2.EndTransactionResponse;
//import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
//import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
//import apache.rocketmq.v2.HeartbeatRequest;
//import apache.rocketmq.v2.HeartbeatResponse;
//import apache.rocketmq.v2.NotifyClientTerminationRequest;
//import apache.rocketmq.v2.NotifyClientTerminationResponse;
//import apache.rocketmq.v2.QueryAssignmentRequest;
//import apache.rocketmq.v2.QueryAssignmentResponse;
//import apache.rocketmq.v2.QueryRouteRequest;
//import apache.rocketmq.v2.QueryRouteResponse;
//import apache.rocketmq.v2.ReceiveMessageRequest;
//import apache.rocketmq.v2.ReceiveMessageResponse;
//import apache.rocketmq.v2.SendMessageRequest;
//import apache.rocketmq.v2.SendMessageResponse;
//import apache.rocketmq.v2.TelemetryCommand;
//import io.grpc.Context;
//import io.grpc.stub.StreamObserver;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import org.apache.rocketmq.common.ThreadFactoryImpl;
//import org.apache.rocketmq.common.constant.LoggerName;
//import org.apache.rocketmq.logging.InternalLogger;
//import org.apache.rocketmq.logging.InternalLoggerFactory;
//import org.apache.rocketmq.proxy.grpc.v2.common.ChannelManager;
//import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
//import org.apache.rocketmq.proxy.common.StartAndShutdown;
//import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
//import org.apache.rocketmq.proxy.service.ServiceManager;
//import org.apache.rocketmq.proxy.service.transaction.TransactionStateCheckRequest;
//import org.apache.rocketmq.proxy.service.transaction.TransactionStateChecker;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.ConsumerService;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.ForwardClientService;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.ProducerService;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.RouteService;
//import org.apache.rocketmq.proxy.grpc.v2.service.cluster.TransactionService;
//import org.apache.rocketmq.proxy.grpc2.v2.GrpcMessingActivity;
//
//public class ClusterGrpcActivity extends AbstractStartAndShutdown implements GrpcMessingActivity {
//    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
//
//    protected final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
//        new ThreadFactoryImpl("ClusterGrpcServiceScheduledThread")
//    );
//
//    protected ChannelManager channelManager;
//    protected ServiceManager serviceManager;
//    protected ProducerService producerService;
//    protected ConsumerService consumerService;
//    protected RouteService routeService;
//    protected ForwardClientService clientService;
//    protected TransactionService transactionService;
//    protected TelemetryCommandManager pollCommandResponseManager;
//    protected GrpcClientManager grpcClientManager;
//
//    public ClusterGrpcActivity() {
//        this.init();
//    }
//
//    protected void init() {
//        this.channelManager = new ChannelManager();
//        this.grpcClientManager = new GrpcClientManager();
//        this.pollCommandResponseManager = new TelemetryCommandManager();
//        this.serviceManager = new ServiceManager(new GrpcTransactionStateChecker());
//        this.consumerService = new ConsumerService(serviceManager, grpcClientManager);
//        this.producerService = new ProducerService(serviceManager);
//        this.routeService = new RouteService(serviceManager, grpcClientManager);
//        this.clientService = new ForwardClientService(serviceManager, scheduledExecutorService,
//            channelManager, grpcClientManager, pollCommandResponseManager);
//        this.transactionService = new TransactionService(serviceManager, channelManager);
//
//        this.appendStartAndShutdown(new ClusterGrpcServiceStartAndShutdown());
//        this.appendStartAndShutdown(this.serviceManager);
//        this.appendStartAndShutdown(this.consumerService);
//        this.appendStartAndShutdown(this.producerService);
//        this.appendStartAndShutdown(this.routeService);
//        this.appendStartAndShutdown(this.clientService);
//        this.appendStartAndShutdown(this.transactionService);
//    }
//
//    @Override
//    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
//        return routeService.queryRoute(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
//        return clientService.heartbeat(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
//        return producerService.sendMessage(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
//        return routeService.queryAssignment(ctx, request);
//    }
//
//    @Override
//    public void receiveMessage(Context ctx, ReceiveMessageRequest request,
//        StreamObserver<ReceiveMessageResponse> responseObserver) {
//        consumerService.receiveMessage(ctx, request, responseObserver);
//    }
//
//    @Override
//    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
//        return consumerService.ackMessage(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
//        ForwardMessageToDeadLetterQueueRequest request) {
//        return producerService.forwardMessageToDeadLetterQueue(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
//        return transactionService.endTransaction(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
//        NotifyClientTerminationRequest request) {
//        return clientService.notifyClientTermination(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
//        ChangeInvisibleDurationRequest request) {
//        return consumerService.changeInvisibleDuration(ctx, request);
//    }
//
//    @Override
//    public StreamObserver<TelemetryCommand> telemetry(Context ctx, StreamObserver<TelemetryCommand> responseObserver) {
//        return clientService.telemetry(ctx, responseObserver);
//    }
//
//    protected class ClusterGrpcServiceStartAndShutdown implements StartAndShutdown {
//
//        @Override
//        public void start() throws Exception {
//        }
//
//        @Override
//        public void shutdown() throws Exception {
//            scheduledExecutorService.shutdown();
//        }
//    }
//
//    protected class GrpcTransactionStateChecker implements TransactionStateChecker {
//
//        @Override
//        public void checkTransactionState(TransactionStateCheckRequest checkData) {
//            transactionService.checkTransactionState(checkData);
//        }
//    }
//}
