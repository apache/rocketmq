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

package org.apache.rocketmq.proxy.grpc;

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
import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
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
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.proxy.grpc.adapter.ResponseWriter;
import org.apache.rocketmq.proxy.grpc.service.GrpcForwardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcMessagingProcessor extends MessagingServiceGrpc.MessagingServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);
    private final GrpcForwardService grpcForwardService;

    public GrpcMessagingProcessor(GrpcForwardService grpcForwardService) {
        this.grpcForwardService = grpcForwardService;
    }

    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        CompletableFuture<QueryRouteResponse> future = grpcForwardService.queryRoute(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        CompletableFuture<HeartbeatResponse> future = grpcForwardService.heartbeat(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        CompletableFuture<HealthCheckResponse> future = grpcForwardService.healthCheck(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        CompletableFuture<SendMessageResponse> future = grpcForwardService.sendMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void queryAssignment(QueryAssignmentRequest request, StreamObserver<QueryAssignmentResponse> responseObserver) {
        CompletableFuture<QueryAssignmentResponse> future = grpcForwardService.queryAssignment(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        CompletableFuture<ReceiveMessageResponse> future = grpcForwardService.receiveMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        CompletableFuture<AckMessageResponse> future = grpcForwardService.ackMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void nackMessage(NackMessageRequest request, StreamObserver<NackMessageResponse> responseObserver) {
        CompletableFuture<NackMessageResponse> future = grpcForwardService.nackMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void forwardMessageToDeadLetterQueue(ForwardMessageToDeadLetterQueueRequest request, StreamObserver<ForwardMessageToDeadLetterQueueResponse> responseObserver) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = grpcForwardService.forwardMessageToDeadLetterQueue(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void endTransaction(EndTransactionRequest request, StreamObserver<EndTransactionResponse> responseObserver) {
        CompletableFuture<EndTransactionResponse> future = grpcForwardService.endTransaction(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void queryOffset(QueryOffsetRequest request, StreamObserver<QueryOffsetResponse> responseObserver) {
        CompletableFuture<QueryOffsetResponse> future = grpcForwardService.queryOffset(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void pullMessage(PullMessageRequest request, StreamObserver<PullMessageResponse> responseObserver) {
        CompletableFuture<PullMessageResponse> future = grpcForwardService.pullMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void pollCommand(PollCommandRequest request, StreamObserver<PollCommandResponse> responseObserver) {
        CompletableFuture<PollCommandResponse> future = grpcForwardService.pollCommand(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void reportThreadStackTrace(ReportThreadStackTraceRequest request, StreamObserver<ReportThreadStackTraceResponse> responseObserver) {
        CompletableFuture<ReportThreadStackTraceResponse> future = grpcForwardService.reportThreadStackTrace(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void reportMessageConsumptionResult(ReportMessageConsumptionResultRequest request, StreamObserver<ReportMessageConsumptionResultResponse> responseObserver) {
        CompletableFuture<ReportMessageConsumptionResultResponse> future = grpcForwardService.reportMessageConsumptionResult(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void notifyClientTermination(NotifyClientTerminationRequest request, StreamObserver<NotifyClientTerminationResponse> responseObserver) {
        CompletableFuture<NotifyClientTerminationResponse> future = grpcForwardService.notifyClientTermination(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }

    @Override
    public void changeInvisibleDuration(ChangeInvisibleDurationRequest request, StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
        CompletableFuture<ChangeInvisibleDurationResponse> future = grpcForwardService.changeInvisibleDuration(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.writeException(responseObserver, e);
                return null;
            });
    }
}
