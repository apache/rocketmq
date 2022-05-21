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

package org.apache.rocketmq.thinclient.rpc;

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
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Client for all explicit RPCs in RocketMQ.
 */
public interface RpcClient {

    /**
     * Record idle seconds without ongoing RPCs
     *
     * @return idle seconds.
     */
    Duration idleDuration();

    /**
     * Shutdown the client. Please <strong>ensure invoked before {@link RpcClient} is garbage collected</strong>.
     *
     * @throws InterruptedException if thread has been interrupted.
     */
    void shutdown() throws InterruptedException;

    /**
     * Query topic route asynchronously.
     *
     * @param metadata gRPC request header metadata.
     * @param request  query route request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of topic route.
     */
    ListenableFuture<QueryRouteResponse> queryRoute(Metadata metadata, QueryRouteRequest request, Executor executor,
        Duration duration);

    /**
     * Heart beat asynchronously.
     *
     * @param metadata gRPC request header metadata.
     * @param request  heart beat request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of heart beat.
     */
    ListenableFuture<HeartbeatResponse> heartbeat(Metadata metadata, HeartbeatRequest request, Executor executor,
        Duration duration);

    /**
     * Send message asynchronously.
     *
     * @param metadata gRPC request header metadata.
     * @param request  send message request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of sending message.
     */
    ListenableFuture<SendMessageResponse> sendMessage(Metadata metadata, SendMessageRequest request, Executor executor,
        Duration duration);

    /**
     * Query assignment asynchronously.
     *
     * @param metadata gRPC request header metadata.
     * @param request  query assignment request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of query assignment.
     */
    ListenableFuture<QueryAssignmentResponse> queryAssignment(Metadata metadata, QueryAssignmentRequest request,
        Executor executor, Duration duration);

    /**
     * Receiving message asynchronously from server.
     *
     * @param metadata gRPC request header metadata.
     * @param request  receiving message request.
     * @param executor gRPC asynchronous executor.
     */
    ListenableFuture<Iterator<ReceiveMessageResponse>> receiveMessage(Metadata metadata, ReceiveMessageRequest request,
        ExecutorService executor, Duration duration);

    /**
     * Ack message asynchronously after success of consumption.
     *
     * @param metadata gRPC request header metadata.
     * @param request  ack message request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of ack message.
     */
    ListenableFuture<AckMessageResponse> ackMessage(Metadata metadata, AckMessageRequest request, Executor executor,
        Duration duration);

    /**
     * Change message invisible duration.
     *
     * @param metadata gRPC request header metadata.
     * @param request  change invisible duration request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of change message invisible duration.
     */
    ListenableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Metadata metadata,
        ChangeInvisibleDurationRequest request, Executor executor, Duration duration);

    /**
     * Send message to dead letter queue asynchronously.
     *
     * @param metadata gRPC request header metadata.
     * @param request  request of sending message to DLQ.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of sending message to DLQ.
     */
    ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        Metadata metadata, ForwardMessageToDeadLetterQueueRequest request, Executor executor, Duration duration);

    /**
     * Submit transaction resolution asynchronously.
     *
     * @param metadata gRPC request header metadata.
     * @param request  end transaction request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of submitting transaction resolution.
     */
    ListenableFuture<EndTransactionResponse> endTransaction(Metadata metadata, EndTransactionRequest request,
        Executor executor, Duration duration);

    /**
     * Asynchronously notify server that client is terminated.
     *
     * @param metadata gRPC request header metadata.
     * @param request  notify client termination request.
     * @param executor gRPC asynchronous executor.
     * @param duration request max duration.
     * @return response future of notification of client termination.
     */
    ListenableFuture<NotifyClientTerminationResponse> notifyClientTermination(Metadata metadata,
        NotifyClientTerminationRequest request, Executor executor, Duration duration);

    StreamObserver<TelemetryCommand> telemetry(Metadata metadata, Executor executor, Duration duration,
        StreamObserver<TelemetryCommand> responseObserver);
}
