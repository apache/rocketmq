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
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthorizationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseWriter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class GrpcMessagingApplication extends MessagingServiceGrpc.MessagingServiceImplBase implements StartAndShutdown {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final GrpcMessingActivity grpcMessingActivity;

    protected final RequestPipeline requestPipeline;

    protected ThreadPoolExecutor routeThreadPoolExecutor;
    protected ThreadPoolExecutor producerThreadPoolExecutor;
    protected ThreadPoolExecutor consumerThreadPoolExecutor;
    protected ThreadPoolExecutor clientManagerThreadPoolExecutor;
    protected ThreadPoolExecutor transactionThreadPoolExecutor;


    protected GrpcMessagingApplication(GrpcMessingActivity grpcMessingActivity, RequestPipeline requestPipeline) {
        this.grpcMessingActivity = grpcMessingActivity;
        this.requestPipeline = requestPipeline;

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
        RequestPipeline pipeline = (context, headers, request) -> {
        };
        // add pipeline
        // the last pipe add will execute at the first
        AuthConfig authConfig = ConfigurationManager.getAuthConfig();
        if (authConfig != null) {
            pipeline = pipeline
                .pipe(new AuthorizationPipeline(authConfig, messagingProcessor))
                .pipe(new AuthenticationPipeline(authConfig, messagingProcessor));
        }
        pipeline = pipeline.pipe(new ContextInitPipeline());
        return new GrpcMessagingApplication(new DefaultGrpcMessingActivity(messagingProcessor), pipeline);
    }

    protected Status flowLimitStatus() {
        return ResponseBuilder.getInstance().buildStatus(Code.TOO_MANY_REQUESTS, "flow limit");
    }

    protected Status convertExceptionToStatus(Throwable t) {
        return ResponseBuilder.getInstance().buildStatus(t);
    }

    protected <V, T> void addExecutor(ExecutorService executor, ProxyContext context, V request, Runnable runnable,
        StreamObserver<T> responseObserver, Function<Status, T> statusResponseCreator) {
        if (request instanceof GeneratedMessageV3) {
            requestPipeline.execute(context, GrpcConstants.METADATA.get(Context.current()), (GeneratedMessageV3) request);
            validateContext(context);
        } else {
            log.error("[BUG]grpc request pipe is not been executed");
        }
        executor.submit(new GrpcTask<>(runnable, context, request, responseObserver, statusResponseCreator.apply(flowLimitStatus())));
    }

    protected <V, T> void writeResponse(ProxyContext context, V request, T response, StreamObserver<T> responseObserver,
        Throwable t, Function<Status, T> errorResponseCreator) {
        if (t != null) {
            ResponseWriter.getInstance().write(
                responseObserver,
                errorResponseCreator.apply(convertExceptionToStatus(t))
            );
        } else {
            ResponseWriter.getInstance().write(responseObserver, response);
        }
    }

    protected ProxyContext createContext() {
        return ProxyContext.create();
    }

    protected void validateContext(ProxyContext context) {
        if (StringUtils.isBlank(context.getClientID())) {
            throw new GrpcProxyException(Code.CLIENT_ID_REQUIRED, "client id cannot be empty");
        }
    }

    @Override
    public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
        Function<Status, QueryRouteResponse> statusResponseCreator = status -> QueryRouteResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.routeThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.queryRoute(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        Function<Status, HeartbeatResponse> statusResponseCreator = status -> HeartbeatResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.clientManagerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.heartbeat(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        Function<Status, SendMessageResponse> statusResponseCreator = status -> SendMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.producerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.sendMessage(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void queryAssignment(QueryAssignmentRequest request,
        StreamObserver<QueryAssignmentResponse> responseObserver) {
        Function<Status, QueryAssignmentResponse> statusResponseCreator = status -> QueryAssignmentResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.routeThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.queryAssignment(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        Function<Status, ReceiveMessageResponse> statusResponseCreator = status -> ReceiveMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.consumerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.receiveMessage(context, request, responseObserver),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        Function<Status, AckMessageResponse> statusResponseCreator = status -> AckMessageResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.consumerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.ackMessage(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void forwardMessageToDeadLetterQueue(ForwardMessageToDeadLetterQueueRequest request,
        StreamObserver<ForwardMessageToDeadLetterQueueResponse> responseObserver) {
        Function<Status, ForwardMessageToDeadLetterQueueResponse> statusResponseCreator = status -> ForwardMessageToDeadLetterQueueResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.producerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.forwardMessageToDeadLetterQueue(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void endTransaction(EndTransactionRequest request, StreamObserver<EndTransactionResponse> responseObserver) {
        Function<Status, EndTransactionResponse> statusResponseCreator = status -> EndTransactionResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.transactionThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.endTransaction(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void notifyClientTermination(NotifyClientTerminationRequest request,
        StreamObserver<NotifyClientTerminationResponse> responseObserver) {
        Function<Status, NotifyClientTerminationResponse> statusResponseCreator = status -> NotifyClientTerminationResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.clientManagerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.notifyClientTermination(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public void changeInvisibleDuration(ChangeInvisibleDurationRequest request,
        StreamObserver<ChangeInvisibleDurationResponse> responseObserver) {
        Function<Status, ChangeInvisibleDurationResponse> statusResponseCreator = status -> ChangeInvisibleDurationResponse.newBuilder().setStatus(status).build();
        ProxyContext context = createContext();
        try {
            this.addExecutor(this.consumerThreadPoolExecutor,
                context,
                request,
                () -> grpcMessingActivity.changeInvisibleDuration(context, request)
                    .whenComplete((response, throwable) -> writeResponse(context, request, response, responseObserver, throwable, statusResponseCreator)),
                responseObserver,
                statusResponseCreator);
        } catch (Throwable t) {
            writeResponse(context, request, null, responseObserver, t, statusResponseCreator);
        }
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        Function<Status, TelemetryCommand> statusResponseCreator = status -> TelemetryCommand.newBuilder().setStatus(status).build();
        ContextStreamObserver<TelemetryCommand> responseTelemetryCommand = grpcMessingActivity.telemetry(responseObserver);
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                ProxyContext context = createContext();
                try {
                    addExecutor(clientManagerThreadPoolExecutor,
                        context,
                        value,
                        () -> responseTelemetryCommand.onNext(context, value),
                        responseObserver,
                        statusResponseCreator);
                } catch (Throwable t) {
                    writeResponse(context, value, null, responseObserver, t, statusResponseCreator);
                }
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

    protected static class GrpcTask<V, T> implements Runnable {

        protected final Runnable runnable;
        protected final ProxyContext context;
        protected final V request;
        protected final T executeRejectResponse;
        protected final StreamObserver<T> streamObserver;

        public GrpcTask(Runnable runnable, ProxyContext context, V request, StreamObserver<T> streamObserver,
            T executeRejectResponse) {
            this.runnable = runnable;
            this.context = context;
            this.streamObserver = streamObserver;
            this.request = request;
            this.executeRejectResponse = executeRejectResponse;
        }

        @Override
        public void run() {
            this.runnable.run();
        }
    }

    protected class GrpcTaskRejectedExecutionHandler implements RejectedExecutionHandler {

        public GrpcTaskRejectedExecutionHandler() {

        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof GrpcTask) {
                try {
                    GrpcTask grpcTask = (GrpcTask) r;
                    writeResponse(grpcTask.context, grpcTask.request, grpcTask.executeRejectResponse, grpcTask.streamObserver, null, null);
                } catch (Throwable t) {
                    log.warn("write rejected error response failed", t);
                }
            }
        }
    }
}
