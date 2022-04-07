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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessagingServiceGrpc;
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
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseWriter;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcForwardService;

public class GrpcMessagingProcessor extends MessagingServiceGrpc.MessagingServiceImplBase {

    private final GrpcForwardService grpcForwardService;

    public GrpcMessagingProcessor(GrpcForwardService grpcForwardService) {
        this.grpcForwardService = grpcForwardService;
    }

    public Status convertExceptionToStatus(Throwable t) {
        if (t instanceof CompletionException) {
            if (t.getCause() instanceof ProxyException) {
                ProxyException proxyException = (ProxyException) t.getCause();
                return ResponseBuilder.buildStatus(proxyException.getCode(), proxyException.getMessage());
            }
        }
        return ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "internal error");
    }

    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        CompletableFuture<QueryRouteResponse> future = grpcForwardService.queryRoute(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    QueryRouteResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        CompletableFuture<HeartbeatResponse> future = grpcForwardService.heartbeat(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    HeartbeatResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        CompletableFuture<SendMessageResponse> future = grpcForwardService.sendMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    SendMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void queryAssignment(QueryAssignmentRequest request,
        StreamObserver<QueryAssignmentResponse> responseObserver) {
        CompletableFuture<QueryAssignmentResponse> future = grpcForwardService.queryAssignment(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    QueryAssignmentResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        CompletableFuture<ReceiveMessageResponse> future = grpcForwardService.receiveMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    ReceiveMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void nackMessage(NackMessageRequest request, StreamObserver<NackMessageResponse> responseObserver) {
        CompletableFuture<NackMessageResponse> future = grpcForwardService.nackMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    NackMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        CompletableFuture<AckMessageResponse> future = grpcForwardService.ackMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    AckMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void forwardMessageToDeadLetterQueue(ForwardMessageToDeadLetterQueueRequest request,
        StreamObserver<ForwardMessageToDeadLetterQueueResponse> responseObserver) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = grpcForwardService.forwardMessageToDeadLetterQueue(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void endTransaction(EndTransactionRequest request, StreamObserver<EndTransactionResponse> responseObserver) {
        CompletableFuture<EndTransactionResponse> future = grpcForwardService.endTransaction(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    EndTransactionResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void queryOffset(QueryOffsetRequest request, StreamObserver<QueryOffsetResponse> responseObserver) {
        CompletableFuture<QueryOffsetResponse> future = grpcForwardService.queryOffset(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    QueryOffsetResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void pullMessage(PullMessageRequest request, StreamObserver<PullMessageResponse> responseObserver) {
        CompletableFuture<PullMessageResponse> future = grpcForwardService.pullMessage(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    PullMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void notifyClientTermination(NotifyClientTerminationRequest request,
        StreamObserver<NotifyClientTerminationResponse> responseObserver) {
        CompletableFuture<NotifyClientTerminationResponse> future = grpcForwardService.notifyClientTermination(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    NotifyClientTerminationResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public void changeInvisibleDuration(ChangeInvisibleDurationRequest request,
        StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
        CompletableFuture<ChangeInvisibleDurationResponse> future = grpcForwardService.changeInvisibleDuration(Context.current(), request);
        future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
            .exceptionally(e -> {
                ResponseWriter.write(
                    responseObserver,
                    ChangeInvisibleDurationResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                );
                return null;
            });
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return grpcForwardService.telemetry(Context.current(), responseObserver);
    }
}
