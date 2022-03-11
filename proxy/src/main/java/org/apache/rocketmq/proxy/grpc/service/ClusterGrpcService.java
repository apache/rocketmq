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
import io.netty.util.concurrent.CompleteFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterGrpcService implements GrpcService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    public ClusterGrpcService() {

    }

    @Override
    public CompleteFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        return null;
    }

    @Override
    public CompleteFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        return null;
    }

    @Override
    public CompleteFuture<HealthCheckResponse> healthCheck(Context ctx, HealthCheckRequest request) {
        return null;
    }

    @Override
    public CompleteFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        return null;
    }

    @Override
    public CompleteFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        return null;
    }

    @Override public CompleteFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        return null;
    }

    @Override public CompleteFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        return null;
    }

    @Override public CompleteFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        return null;
    }

    @Override
    public CompleteFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return null;
    }

    @Override public CompleteFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        return null;
    }

    @Override public CompleteFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        return null;
    }

    @Override public CompleteFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        return null;
    }

    @Override public CompleteFuture<PollCommandResponse> pollCommand(Context ctx, PollCommandRequest request) {
        return null;
    }

    @Override public CompleteFuture<ReportThreadStackTraceResponse> reportThreadStackTrace(Context ctx,
        ReportThreadStackTraceRequest request) {
        return null;
    }

    @Override public CompleteFuture<ReportMessageConsumptionResultResponse> reportMessageConsumptionResult(Context ctx,
        ReportMessageConsumptionResultRequest request) {
        return null;
    }

    @Override public CompleteFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
        NotifyClientTerminationRequest request) {
        return null;
    }

    @Override public CompleteFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        return null;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void shutdown() throws Exception {
    }
}
