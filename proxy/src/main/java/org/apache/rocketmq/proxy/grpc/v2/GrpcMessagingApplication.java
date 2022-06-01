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
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcMessagingApplication extends MessagingServiceGrpc.MessagingServiceImplBase implements StartAndShutdown {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final GrpcMessingActivity grpcMessingActivity;

    protected ThreadPoolExecutor routeThreadPoolExecutor;
    protected ThreadPoolExecutor producerThreadPoolExecutor;
    protected ThreadPoolExecutor consumerThreadPoolExecutor;
    protected ThreadPoolExecutor clientManagerThreadPoolExecutor;
    protected ThreadPoolExecutor transactionThreadPoolExecutor;

    protected GrpcMessagingApplication(GrpcMessingActivity grpcMessingActivity) {
        this.grpcMessingActivity = grpcMessingActivity;

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.routeThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcRouteThreadPoolNums(),
            config.getGrpcRouteThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcRouteThreadPool",
            config.getGrpcRouteThreadQueueCapacity()
        );
        this.producerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcProducerThreadPoolNums(),
            config.getGrpcProducerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcProducerThreadPool",
            config.getGrpcProducerThreadQueueCapacity()
        );
        this.consumerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcConsumerThreadPoolNums(),
            config.getGrpcConsumerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcConsumerThreadPool",
            config.getGrpcConsumerThreadQueueCapacity()
        );
        this.clientManagerThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcClientManagerThreadPoolNums(),
            config.getGrpcClientManagerThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcClientManagerThreadPool",
            config.getGrpcClientManagerThreadQueueCapacity()
        );
        this.transactionThreadPoolExecutor = ThreadPoolMonitor.createAndMonitor(
            config.getGrpcTransactionThreadPoolNums(),
            config.getGrpcTransactionThreadPoolNums(),
            1,
            TimeUnit.MINUTES,
            "GrpcTransactionThreadPool",
            config.getGrpcTransactionThreadQueueCapacity()
        );

        this.init();
    }

    protected void init() {
        GrpcTaskRejectedExecutionHandler rejectedExecutionHandler = new GrpcTaskRejectedExecutionHandler();
        this.routeThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.routeThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.producerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.consumerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.clientManagerThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
        this.transactionThreadPoolExecutor.setRejectedExecutionHandler(rejectedExecutionHandler);
    }

    public static GrpcMessagingApplication create(MessagingProcessor messagingProcessor) {
        return new GrpcMessagingApplication(new DefaultGrpcMessingActivity(
            messagingProcessor
        ));
    }

    protected Status flowLimitStatus() {
        return ResponseBuilder.buildStatus(Code.TOO_MANY_REQUESTS, "flow limit");
    }

    protected Status convertExceptionToStatus(Throwable t) {
        return ResponseBuilder.buildStatus(t);
    }

    protected <T> void addExecutor(ExecutorService executor, Runnable runnable, StreamObserver<T> responseObserver,
        T executeRejectResponse) {
        executor.submit(new GrpcTask<T>(runnable, responseObserver, executeRejectResponse));
    }

    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.routeThreadPoolExecutor,
            () -> {
                CompletableFuture<QueryRouteResponse> future = grpcMessingActivity.queryRoute(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            QueryRouteResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            QueryRouteResponse.newBuilder().setStatus(flowLimitStatus()).build());
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.clientManagerThreadPoolExecutor,
            () -> {
                CompletableFuture<HeartbeatResponse> future = grpcMessingActivity.heartbeat(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            HeartbeatResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            HeartbeatResponse.newBuilder().setStatus(flowLimitStatus()).build());
    }

    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.producerThreadPoolExecutor,
            () -> {
                CompletableFuture<SendMessageResponse> future = grpcMessingActivity.sendMessage(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            SendMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            SendMessageResponse.newBuilder().setStatus(flowLimitStatus()).build());
    }

    @Override
    public void queryAssignment(QueryAssignmentRequest request,
        StreamObserver<QueryAssignmentResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.routeThreadPoolExecutor,
            () -> {
                CompletableFuture<QueryAssignmentResponse> future = grpcMessingActivity.queryAssignment(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            QueryAssignmentResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            QueryAssignmentResponse.newBuilder().setStatus(flowLimitStatus()).build());
    }

    @Override
    public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.consumerThreadPoolExecutor,
            () -> grpcMessingActivity.receiveMessage(ctx, request, responseObserver),
            responseObserver,
            ReceiveMessageResponse.newBuilder().setStatus(flowLimitStatus()).build());

    }

    @Override
    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.consumerThreadPoolExecutor,
            () -> {
                CompletableFuture<AckMessageResponse> future = grpcMessingActivity.ackMessage(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            AckMessageResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            AckMessageResponse.newBuilder().setStatus(flowLimitStatus()).build());

    }

    @Override
    public void forwardMessageToDeadLetterQueue(ForwardMessageToDeadLetterQueueRequest request,
        StreamObserver<ForwardMessageToDeadLetterQueueResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.producerThreadPoolExecutor,
            () -> {
                CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = grpcMessingActivity.forwardMessageToDeadLetterQueue(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(flowLimitStatus()).build());
    }

    @Override
    public void endTransaction(EndTransactionRequest request, StreamObserver<EndTransactionResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.transactionThreadPoolExecutor,
            () -> {
                CompletableFuture<EndTransactionResponse> future = grpcMessingActivity.endTransaction(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            EndTransactionResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            EndTransactionResponse.newBuilder().setStatus(flowLimitStatus()).build());
    }

    @Override
    public void notifyClientTermination(NotifyClientTerminationRequest request,
        StreamObserver<NotifyClientTerminationResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.clientManagerThreadPoolExecutor,
            () -> {
                CompletableFuture<NotifyClientTerminationResponse> future = grpcMessingActivity.notifyClientTermination(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            NotifyClientTerminationResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            NotifyClientTerminationResponse.newBuilder().setStatus(flowLimitStatus()).build());

    }

    @Override
    public void changeInvisibleDuration(ChangeInvisibleDurationRequest request,
        StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
        Context ctx = Context.current();
        this.addExecutor(this.consumerThreadPoolExecutor,
            () -> {
                CompletableFuture<ChangeInvisibleDurationResponse> future = grpcMessingActivity.changeInvisibleDuration(ctx, request);
                future.thenAccept(response -> ResponseWriter.write(responseObserver, response))
                    .exceptionally(e -> {
                        ResponseWriter.write(
                            responseObserver,
                            ChangeInvisibleDurationResponse.newBuilder().setStatus(convertExceptionToStatus(e)).build()
                        );
                        return null;
                    });
            },
            responseObserver,
            ChangeInvisibleDurationResponse.newBuilder().setStatus(flowLimitStatus()).build());

    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        StreamObserver<TelemetryCommand> responseTelemetryCommand = grpcMessingActivity.telemetry(Context.current(), responseObserver);
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                addExecutor(clientManagerThreadPoolExecutor,
                    () -> responseTelemetryCommand.onNext(value),
                    responseObserver,
                    TelemetryCommand.newBuilder().setStatus(flowLimitStatus()).build());
            }

            @Override
            public void onError(Throwable t) {
                responseTelemetryCommand.onError(t);
            }

            @Override
            public void onCompleted() {
                responseTelemetryCommand.onCompleted();
            }
        };
    }

    @Override
    public void shutdown() throws Exception {
        this.grpcMessingActivity.shutdown();

        this.routeThreadPoolExecutor.shutdown();
        this.routeThreadPoolExecutor.shutdown();
        this.producerThreadPoolExecutor.shutdown();
        this.consumerThreadPoolExecutor.shutdown();
        this.clientManagerThreadPoolExecutor.shutdown();
        this.transactionThreadPoolExecutor.shutdown();
    }

    @Override
    public void start() throws Exception {
        this.grpcMessingActivity.start();
    }

    protected static class GrpcTask<T> implements Runnable {

        private final Runnable runnable;
        private final T executeRejectResponse;
        private final StreamObserver<T> streamObserver;

        public GrpcTask(Runnable runnable, StreamObserver<T> streamObserver, T executeRejectResponse) {
            this.runnable = runnable;
            this.streamObserver = streamObserver;
            this.executeRejectResponse = executeRejectResponse;
        }

        @Override
        public void run() {
            this.runnable.run();
        }
    }

    protected static class GrpcTaskRejectedExecutionHandler implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof GrpcTask) {
                try {
                    GrpcTask grpcTask = (GrpcTask) r;
                    ResponseWriter.write(grpcTask.streamObserver, grpcTask.executeRejectResponse);
                } catch (Throwable t) {
                    log.warn("write rejected error response failed", t);
                }
            }
        }
    }
}
