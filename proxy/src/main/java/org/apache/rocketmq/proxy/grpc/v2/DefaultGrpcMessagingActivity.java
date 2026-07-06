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
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminActivity;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminContextFactory;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminCoordinatorService;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminEndpointExecutor;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminEndpointHandler;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminInProcessPeerMessageTransport;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerClient;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerGrpcService;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerLocalExecutor;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerMessageClient;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerMessageHandler;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminScopeRouter;
import org.apache.rocketmq.proxy.grpc.v2.admin.TimedProxyClientAdminPeerClient;
import org.apache.rocketmq.proxy.grpc.v2.client.ClientActivity;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.consumer.AckMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ChangeInvisibleDurationActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.ForwardMessageToDLQActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.RecallMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.SendMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.route.RouteActivity;
import org.apache.rocketmq.proxy.grpc.v2.transaction.EndTransactionActivity;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminAuthorizationService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminAuthorizationService;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.MeteredAuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceCleaner;

public class DefaultGrpcMessagingActivity extends AbstractStartAndShutdown implements GrpcMessagingActivity {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected GrpcClientSettingsManager grpcClientSettingsManager;
    protected GrpcChannelManager grpcChannelManager;
    protected ReceiveMessageActivity receiveMessageActivity;
    protected AckMessageActivity ackMessageActivity;
    protected ChangeInvisibleDurationActivity changeInvisibleDurationActivity;
    protected SendMessageActivity sendMessageActivity;
    protected RecallMessageActivity recallMessageActivity;
    protected ForwardMessageToDLQActivity forwardMessageToDLQActivity;
    protected EndTransactionActivity endTransactionActivity;
    protected RouteActivity routeActivity;
    protected ClientActivity clientActivity;
    protected ProxyClientReadService proxyClientReadService;
    protected ProxyClientReadServiceCleaner proxyClientReadServiceCleaner;
    protected ClientAdminService clientAdminService;
    protected AuthorizingClientAdminService authorizingClientAdminService;
    protected ProxyClientAdminActivity proxyClientAdminActivity;
    protected ProxyClientAdminPeerClient proxyClientAdminPeerClient;
    protected ProxyClientAdminPeerGrpcService proxyClientAdminPeerGrpcService;
    protected ExecutorService proxyClientAdminPeerExecutor;
    protected ProxyClientAdminScopeRouter proxyClientAdminScopeRouter;
    protected ProxyClientAdminContextFactory proxyClientAdminContextFactory;
    protected ProxyClientAdminEndpointHandler proxyClientAdminEndpointHandler;
    protected ProxyClientAdminEndpointExecutor proxyClientAdminEndpointExecutor;

    protected DefaultGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
        this.init(messagingProcessor);
    }

    protected void init(MessagingProcessor messagingProcessor) {
        this.grpcClientSettingsManager = new GrpcClientSettingsManager(messagingProcessor);
        this.grpcChannelManager = new GrpcChannelManager(messagingProcessor.getProxyRelayService(), this.grpcClientSettingsManager);
        this.proxyClientReadService = new ProxyClientReadService(ProxyMetricsManager::recordProxyClientReadModelOperation);
        this.clientAdminService = new DefaultClientAdminService(this.proxyClientReadService);
        ClientAdminAuthorizationService clientAdminAuthorizationService = new DefaultClientAdminAuthorizationService(
            ConfigurationManager.getAuthConfig(),
            messagingProcessor::getMetadataService
        );
        this.authorizingClientAdminService = new MeteredAuthorizingClientAdminService(
            this.clientAdminService,
            clientAdminAuthorizationService,
            ProxyMetricsManager::recordProxyClientAdminRequest
        );
        this.proxyClientAdminActivity = new ProxyClientAdminActivity(this.authorizingClientAdminService);
        boolean enableCrossProxyQuery = ConfigurationManager.getProxyConfig()
            .isEnableProxyClientAdminCrossProxyQuery();
        ProxyClientAdminCoordinatorService proxyClientAdminCoordinatorService = null;
        String crossProxyLocalProxyId = null;
        if (enableCrossProxyQuery) {
            crossProxyLocalProxyId = this.requireCrossProxyLocalProxyId();
            long peerRequestTimeoutMillis = this.requireCrossProxyPeerRequestTimeoutMillis();
            this.proxyClientAdminPeerExecutor = ThreadUtils.newSingleThreadExecutor(
                new ThreadFactoryImpl("ProxyClientAdminPeerClient_")
            );
            this.proxyClientAdminPeerClient = this.createProxyClientAdminPeerClient(
                crossProxyLocalProxyId,
                this.proxyClientAdminActivity,
                this.proxyClientAdminPeerExecutor,
                peerRequestTimeoutMillis
            );
            this.appendShutdown(this.proxyClientAdminPeerExecutor::shutdown);
            proxyClientAdminCoordinatorService = new ProxyClientAdminCoordinatorService(this.proxyClientAdminPeerClient);
        }
        this.proxyClientAdminScopeRouter = new ProxyClientAdminScopeRouter(
            this.proxyClientAdminActivity,
            proxyClientAdminCoordinatorService,
            enableCrossProxyQuery,
            clientAdminAuthorizationService,
            ProxyMetricsManager::recordProxyClientAdminRequest
        );
        this.proxyClientAdminContextFactory =
            GrpcRequestPipelineFactory.createProxyClientAdminContextFactory(messagingProcessor);
        if (enableCrossProxyQuery) {
            this.proxyClientAdminPeerGrpcService = this.createProxyClientAdminPeerGrpcService(
                crossProxyLocalProxyId,
                this.clientAdminService,
                this.proxyClientAdminContextFactory
            );
        }
        this.proxyClientAdminEndpointHandler = new ProxyClientAdminEndpointHandler(this.proxyClientAdminScopeRouter);
        this.proxyClientAdminEndpointExecutor = new ProxyClientAdminEndpointExecutor(
            this.proxyClientAdminContextFactory,
            this.proxyClientAdminEndpointHandler
        );
        ProxyMetricsManager.setProxyClientReadServiceStatsSupplier(this.proxyClientReadService::snapshotStats);
        this.appendShutdown(() -> ProxyMetricsManager.setProxyClientReadServiceStatsSupplier(null));

        this.receiveMessageActivity = new ReceiveMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.ackMessageActivity = new AckMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.changeInvisibleDurationActivity = new ChangeInvisibleDurationActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.sendMessageActivity = new SendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.recallMessageActivity = new RecallMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.forwardMessageToDLQActivity = new ForwardMessageToDLQActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.endTransactionActivity = new EndTransactionActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.routeActivity = new RouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.clientActivity = new ClientActivity(
            messagingProcessor,
            grpcClientSettingsManager,
            grpcChannelManager,
            proxyClientReadService
        );

        this.appendStartAndShutdown(this.grpcClientSettingsManager);
        this.proxyClientReadServiceCleaner = this.createProxyClientReadServiceCleaner();
        if (this.proxyClientReadServiceCleaner != null) {
            this.appendStartAndShutdown(this.proxyClientReadServiceCleaner);
        }
    }

    protected ProxyClientReadServiceCleaner createProxyClientReadServiceCleaner() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        if (!proxyConfig.isEnableProxyClientReadServiceCleaner()) {
            return null;
        }
        ScheduledExecutorService scheduledExecutorService = ThreadUtils.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("ProxyClientReadServiceCleaner_")
        );
        return new ProxyClientReadServiceCleaner(
            this.proxyClientReadService,
            proxyConfig.getProxyClientReadServiceCleanerInactiveTimeoutMillis(),
            proxyConfig.getProxyClientReadServiceCleanerIntervalMillis(),
            scheduledExecutorService,
            System::currentTimeMillis
        );
    }

    protected String localProxyId() {
        return StringUtils.defaultIfBlank(
            StringUtils.trimToNull(ConfigurationManager.getProxyConfig().getProxyName()),
            "DEFAULT_PROXY"
        );
    }

    private String requireCrossProxyLocalProxyId() {
        String localProxyId = StringUtils.trimToNull(ConfigurationManager.getProxyConfig().getProxyName());
        if (localProxyId == null) {
            throw new IllegalArgumentException(
                "proxyName is required when proxy client admin cross-proxy query is enabled"
            );
        }
        return localProxyId;
    }

    protected ProxyClientAdminPeerClient createProxyClientAdminPeerClient(String localProxyId,
        ProxyClientAdminActivity proxyClientAdminActivity, ExecutorService executorService, long timeoutMillis) {
        ProxyClientAdminPeerLocalExecutor localPeerExecutor = this.createProxyClientAdminPeerLocalExecutor(
            localProxyId,
            this.clientAdminService
        );
        ProxyClientAdminPeerMessageHandler localPeerMessageHandler =
            new ProxyClientAdminPeerMessageHandler(localPeerExecutor);
        return new TimedProxyClientAdminPeerClient(
            new ProxyClientAdminPeerMessageClient(
                new ProxyClientAdminInProcessPeerMessageTransport(Collections.singletonMap(
                    localProxyId,
                    localPeerMessageHandler
                ))
            ),
            executorService,
            timeoutMillis
        );
    }

    protected ProxyClientAdminPeerLocalExecutor createProxyClientAdminPeerLocalExecutor(String localProxyId,
        ClientAdminService clientAdminService) {
        return new ProxyClientAdminPeerLocalExecutor(localProxyId, clientAdminService);
    }

    protected ProxyClientAdminPeerGrpcService createProxyClientAdminPeerGrpcService(String localProxyId,
        ClientAdminService clientAdminService, ProxyClientAdminContextFactory contextFactory) {
        ProxyClientAdminPeerLocalExecutor localPeerExecutor = this.createProxyClientAdminPeerLocalExecutor(
            localProxyId,
            clientAdminService
        );
        return new ProxyClientAdminPeerGrpcService(
            contextFactory,
            new ProxyClientAdminPeerMessageHandler(localPeerExecutor)
        );
    }

    private long requireCrossProxyPeerRequestTimeoutMillis() {
        long peerRequestTimeoutMillis =
            ConfigurationManager.getProxyConfig().getProxyClientAdminPeerRequestTimeoutMillis();
        if (peerRequestTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                "proxyClientAdminPeerRequestTimeoutMillis must be positive when proxy client admin "
                    + "cross-proxy query is enabled"
            );
        }
        return peerRequestTimeoutMillis;
    }

    protected ClientAdminService getClientAdminService() {
        return this.clientAdminService;
    }

    protected AuthorizingClientAdminService getAuthorizingClientAdminService() {
        return this.authorizingClientAdminService;
    }

    public ProxyClientAdminActivity getProxyClientAdminActivity() {
        return this.proxyClientAdminActivity;
    }

    public ProxyClientAdminScopeRouter getProxyClientAdminScopeRouter() {
        return this.proxyClientAdminScopeRouter;
    }

    public ProxyClientAdminEndpointHandler getProxyClientAdminEndpointHandler() {
        return this.proxyClientAdminEndpointHandler;
    }

    public ProxyClientAdminEndpointExecutor getProxyClientAdminEndpointExecutor() {
        return this.proxyClientAdminEndpointExecutor;
    }

    public ProxyClientAdminPeerGrpcService getProxyClientAdminPeerGrpcService() {
        return this.proxyClientAdminPeerGrpcService;
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return this.routeActivity.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        return this.clientActivity.heartbeat(ctx, request);
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        return this.sendMessageActivity.sendMessage(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        return this.routeActivity.queryAssignment(ctx, request);
    }

    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        this.receiveMessageActivity.receiveMessage(ctx, request, responseObserver);
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        return this.ackMessageActivity.ackMessage(ctx, request);
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return this.forwardMessageToDLQActivity.forwardMessageToDeadLetterQueue(ctx, request);
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request) {
        return this.endTransactionActivity.endTransaction(ctx, request);
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        return this.clientActivity.notifyClientTermination(ctx, request);
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        return this.changeInvisibleDurationActivity.changeInvisibleDuration(ctx, request);
    }

    @Override
    public CompletableFuture<RecallMessageResponse> recallMessage(ProxyContext ctx,
        RecallMessageRequest request) {
        return this.recallMessageActivity.recallMessage(ctx, request);
    }

    @Override
    public CompletableFuture<SyncLiteSubscriptionResponse> syncLiteSubscription(ProxyContext ctx,
        SyncLiteSubscriptionRequest request) {
        return this.clientActivity.syncLiteSubscription(ctx, request);
    }

    @Override
    public ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return this.clientActivity.telemetry(responseObserver);
    }
}
