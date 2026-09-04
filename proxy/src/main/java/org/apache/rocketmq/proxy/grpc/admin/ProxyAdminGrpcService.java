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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.AdminGrpc;
import apache.rocketmq.v2.ClientInfo;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ConsumerRunningInfo;
import apache.rocketmq.v2.DescribeGroupAccumulationRequest;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse;
import apache.rocketmq.v2.DescribeSubscriptionRequest;
import apache.rocketmq.v2.DescribeSubscriptionResponse;
import apache.rocketmq.v2.DescribeTopicStatusRequest;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.DeleteSubscriptionRequest;
import apache.rocketmq.v2.DeleteSubscriptionResponse;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.GetConsumerRunningInfoRequest;
import apache.rocketmq.v2.GetConsumerRunningInfoResponse;
import apache.rocketmq.v2.GetProxyRuntimeStatsRequest;
import apache.rocketmq.v2.GetProxyRuntimeStatsResponse;
import apache.rocketmq.v2.GetTopicRouteRequest;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.ListConsumerConnectionRequest;
import apache.rocketmq.v2.ListConsumerConnectionResponse;
import apache.rocketmq.v2.ListMessageRequest;
import apache.rocketmq.v2.ListMessageResponse;
import apache.rocketmq.v2.ListSubscriptionRequest;
import apache.rocketmq.v2.ListSubscriptionResponse;
import apache.rocketmq.v2.PrintThreadStackTraceRequest;
import apache.rocketmq.v2.PrintThreadStackTraceResponse;
import apache.rocketmq.v2.QueryTimeSpanRequest;
import apache.rocketmq.v2.QueryTimeSpanResponse;
import apache.rocketmq.v2.ChangeLogLevelRequest;
import apache.rocketmq.v2.ChangeLogLevelResponse;
import apache.rocketmq.v2.ResetGroupOffsetRequest;
import apache.rocketmq.v2.ResetGroupOffsetResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.VerifyMessageRequest;
import apache.rocketmq.v2.VerifyMessageResponse;
import apache.rocketmq.v2.AdminSendMessageRequest;
import apache.rocketmq.v2.AdminSendMessageResponse;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SubscriptionInfo;
import apache.rocketmq.v2.UA;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;

/**
 * RIP-2 Proxy Admin gRPC service.
 *
 * <p>Every capability is served from the proxy process itself (via the gRPC proxy):
 * <ul>
 *   <li>Online clients / connections are read from the proxy's own
 *       {@link GrpcChannelManager} (the authority on gRPC clients connected to the proxy).</li>
 *   <li>Runtime stats, log level and topic route are proxy-owned state.</li>
 *   <li>Subscriptions are derived from the client settings tracked by the proxy.</li>
 *   <li>Client diagnostics (thread stack / verify / running info) are dispatched through the
 *       proxy's own telemetry channel to the target client.</li>
 *   <li>Broker-internal data (offsets, accumulation, reset, message query) is reached ONLY through
 *       the proxy's own {@link AdminService} gateway — i.e. the proxy's managed broker client.
 *       The admin code never opens a direct link to the broker.</li>
 * </ul>
 *
 * <p>This class is protocol-pure: it only depends on the RIP-2 gRPC contract
 * ({@code apache.rocketmq.v2.*}, generated from rocketmq-apis). The translation between the
 * broker's internal wire types and the v2 protocol lives in {@link AdminModelConverter}.
 */
public class ProxyAdminGrpcService extends AdminGrpc.AdminImplBase {

    private static final Logger log = LoggerFactory.getLogger(ProxyAdminGrpcService.class);
    private static final long DEFAULT_TIMEOUT_MILLIS = 3000L;
    private static final String PROXY_NAME = "rocketmq-proxy";
    private static final String PROXY_VERSION = "5.5.0";

    private final ServiceManager serviceManager;
    private final MessagingProcessor messagingProcessor;
    private final GrpcChannelManager grpcChannelManager;
    private final GrpcClientSettingsManager grpcClientSettingsManager;

    public ProxyAdminGrpcService(ServiceManager serviceManager, MessagingProcessor messagingProcessor,
        GrpcChannelManager grpcChannelManager, GrpcClientSettingsManager grpcClientSettingsManager) {
        this.serviceManager = serviceManager;
        this.messagingProcessor = messagingProcessor;
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private Status ok() {
        return Status.newBuilder().setCode(Code.OK).build();
    }

    private Status fail(Code code, String message) {
        return Status.newBuilder().setCode(code).setMessage(message).build();
    }

    private ProxyContext ctx() {
        return ProxyContext.create();
    }

    private String resolveBrokerAddr(String topic) throws Exception {
        org.apache.rocketmq.proxy.service.route.MessageQueueView mqv =
            serviceManager.getTopicRouteService().getAllMessageQueueView(ctx(), topic);
        if (mqv == null || mqv.getReadSelector() == null || mqv.getReadSelector().getQueues().isEmpty()) {
            throw new RuntimeException("topic route not found for " + topic);
        }
        org.apache.rocketmq.proxy.service.route.AddressableMessageQueue mq =
            mqv.getReadSelector().getQueues().get(0);
        String brokerAddr = mq.getBrokerAddr();
        if (brokerAddr == null || brokerAddr.isEmpty()) {
            throw new RuntimeException("broker address not found for topic " + topic);
        }
        return brokerAddr;
    }

    /**
     * RIP-2 fix: resolve ALL distinct broker addresses hosting the topic, so multi-broker
     * clusters get complete data for accumulation/reset/query/delete operations.
     */
    private List<String> resolveBrokerAddrs(String topic) throws Exception {
        org.apache.rocketmq.proxy.service.route.MessageQueueView mqv =
            serviceManager.getTopicRouteService().getAllMessageQueueView(ctx(), topic);
        if (mqv == null || mqv.getReadSelector() == null || mqv.getReadSelector().getQueues().isEmpty()) {
            throw new RuntimeException("topic route not found for " + topic);
        }
        java.util.LinkedHashSet<String> addrs = new java.util.LinkedHashSet<>();
        for (org.apache.rocketmq.proxy.service.route.AddressableMessageQueue mq : mqv.getReadSelector().getQueues()) {
            if (mq.getBrokerAddr() != null && !mq.getBrokerAddr().isEmpty()) {
                addrs.add(mq.getBrokerAddr());
            }
        }
        if (addrs.isEmpty()) {
            throw new RuntimeException("broker address not found for topic " + topic);
        }
        return new ArrayList<>(addrs);
    }


    private ClientInfo buildClientInfo(GrpcClientChannel channel) {
        String clientId = channel.getClientId();
        ClientInfo.Builder builder = ClientInfo.newBuilder().setClientId(clientId);
        apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
        if (settings != null) {
            UA ua = settings.getUserAgent();
            if (ua != null) {
                builder.setVersion(ua.getVersion());
                if (ua.getLanguage() != null) {
                    builder.setLanguage(ua.getLanguage().name());
                }
                builder.setHostname(ua.getHostname());
            }
        }
        String remoteAddress = channel.getRemoteAddress();
        if (remoteAddress != null && !remoteAddress.isEmpty()) {
            builder.setEgressIp(remoteAddress);
        }
        return builder.build();
    }

    private boolean matchGroup(apache.rocketmq.v2.Settings settings, Resource group) {
        if (group.getName().isEmpty()) {
            return true;
        }
        if (settings == null || !settings.hasSubscription()) {
            return false;
        }
        return group.getName().equals(settings.getSubscription().getGroup().getName());
    }

    private List<ClientInfo> onlineConsumers(Resource group) {
        List<ClientInfo> result = new ArrayList<>();
        Collection<GrpcClientChannel> channels = grpcChannelManager.getClientChannels();
        for (GrpcClientChannel channel : channels) {
            apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(channel.getClientId());
            if (settings == null || settings.getClientType() == apache.rocketmq.v2.ClientType.CLIENT_TYPE_UNSPECIFIED) {
                continue;
            }
            if (!matchGroup(settings, group)) {
                continue;
            }
            result.add(buildClientInfo(channel));
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // RIP-2 RPCs
    // -------------------------------------------------------------------------

    @Override
    public void changeLogLevel(ChangeLogLevelRequest request, StreamObserver<ChangeLogLevelResponse> responseObserver) {
        String remark;
        try {
            // The proxy is wired to logback (rocketmq-logback-classic); change the root logger level
            // through the relocated logback API. No broker link is involved.
            org.apache.rocketmq.logging.org.slf4j.ILoggerFactory factory =
                org.apache.rocketmq.logging.org.slf4j.LoggerFactory.getILoggerFactory();
            if (factory instanceof org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext) {
                org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext loggerContext =
                    (org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext) factory;
                org.apache.rocketmq.logging.ch.qos.logback.classic.Level level =
                    org.apache.rocketmq.logging.ch.qos.logback.classic.Level.toLevel(request.getLevel().name());
                loggerContext.getLogger(
                        org.apache.rocketmq.logging.ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME)
                    .setLevel(level);
                remark = "log level changed to " + level;
            } else {
                remark = "unsupported logging backend, cannot change log level at runtime";
            }
        } catch (Throwable t) {
            remark = "failed to change log level: " + t.getMessage();
            log.warn("changeLogLevel failed", t);
        }
        responseObserver.onNext(ChangeLogLevelResponse.newBuilder().setRemark(remark).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getProxyRuntimeStats(GetProxyRuntimeStatsRequest request,
        StreamObserver<GetProxyRuntimeStatsResponse> responseObserver) {
        int producers = 0;
        int consumers = 0;
        for (GrpcClientChannel channel : grpcChannelManager.getClientChannels()) {
            apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(channel.getClientId());
            if (settings == null) {
                continue;
            }
            switch (settings.getClientType()) {
                case PRODUCER:
                case LITE_PUSH_CONSUMER:
                    producers++;
                    break;
                case PUSH_CONSUMER:
                case SIMPLE_CONSUMER:
                case PULL_CONSUMER:
                case LITE_SIMPLE_CONSUMER:
                    consumers++;
                    break;
                default:
                    break;
            }
        }
        GetProxyRuntimeStatsResponse.Builder builder = GetProxyRuntimeStatsResponse.newBuilder()
            .setProxyName(PROXY_NAME)
            .setVersion(PROXY_VERSION)
            .setConnections(grpcChannelManager.getClientChannels().size())
            .setProducers(producers)
            .setConsumers(consumers);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getTopicRoute(GetTopicRouteRequest request, StreamObserver<GetTopicRouteResponse> responseObserver) {
        try {
            GetTopicRouteResponse response = AdminModelConverter.toTopicRoute(
                serviceManager.getAdminService(), request.getTopic().getName());
            responseObserver.onNext(response);
        } catch (Throwable t) {
            log.warn("getTopicRoute failed", t);
            responseObserver.onNext(GetTopicRouteResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage()))
                .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void describeTopicStatus(DescribeTopicStatusRequest request,
        StreamObserver<DescribeTopicStatusResponse> responseObserver) {
        try {
            String topic = request.getTopic().getName();
            String brokerAddr = resolveBrokerAddr(topic);
            DescribeTopicStatusResponse response = AdminModelConverter.toTopicStatus(
                serviceManager.getAdminService(), brokerAddr, topic, DEFAULT_TIMEOUT_MILLIS);
            responseObserver.onNext(response);
        } catch (Throwable t) {
            log.warn("describeTopicStatus failed", t);
            responseObserver.onNext(DescribeTopicStatusResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void listSubscription(ListSubscriptionRequest request, StreamObserver<ListSubscriptionResponse> responseObserver) {
        try {
            ListSubscriptionResponse.Builder builder = ListSubscriptionResponse.newBuilder().setStatus(ok());
            Resource topicFilter = request.getTopic();
            Resource groupFilter = request.getGroup();
            for (GrpcClientChannel channel : grpcChannelManager.getClientChannels()) {
                apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(channel.getClientId());
                if (settings == null || !settings.hasSubscription()) {
                    continue;
                }
                apache.rocketmq.v2.Subscription subscription = settings.getSubscription();
                String group = subscription.getGroup().getName();
                if (!groupFilter.getName().isEmpty() && !group.equals(groupFilter.getName())) {
                    continue;
                }
                for (apache.rocketmq.v2.SubscriptionEntry entry : subscription.getSubscriptionsList()) {
                    String entryTopic = entry.hasTopic() ? entry.getTopic().getName() : "";
                    if (!topicFilter.getName().isEmpty() && !entryTopic.equals(topicFilter.getName())) {
                        continue;
                    }
                    SubscriptionInfo.Builder info = SubscriptionInfo.newBuilder()
                        .setGroup(resource(group))
                        .setTopic(resource(entryTopic));
                    if (entry.hasExpression()) {
                        info.setExpression(entry.getExpression());
                    }
                    info.setOnline(true);
                    builder.addSubscriptionInfo(info);
                }
            }
            responseObserver.onNext(builder.build());
        } catch (Throwable t) {
            log.warn("listSubscription failed", t);
            responseObserver.onNext(ListSubscriptionResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void describeSubscription(DescribeSubscriptionRequest request,
        StreamObserver<DescribeSubscriptionResponse> responseObserver) {
        try {
            DescribeSubscriptionResponse.Builder builder = DescribeSubscriptionResponse.newBuilder().setStatus(ok());
            Resource topicFilter = request.getTopic();
            Resource groupFilter = request.getGroup();
            for (GrpcClientChannel channel : grpcChannelManager.getClientChannels()) {
                apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(channel.getClientId());
                if (settings == null || !settings.hasSubscription()) {
                    continue;
                }
                apache.rocketmq.v2.Subscription subscription = settings.getSubscription();
                String group = subscription.getGroup().getName();
                if (!groupFilter.getName().isEmpty() && !group.equals(groupFilter.getName())) {
                    continue;
                }
                for (apache.rocketmq.v2.SubscriptionEntry entry : subscription.getSubscriptionsList()) {
                    String entryTopic = entry.hasTopic() ? entry.getTopic().getName() : "";
                    if (!topicFilter.getName().isEmpty() && !entryTopic.equals(topicFilter.getName())) {
                        continue;
                    }
                    SubscriptionInfo.Builder info = SubscriptionInfo.newBuilder()
                        .setGroup(resource(group))
                        .setTopic(resource(entryTopic));
                    if (entry.hasExpression()) {
                        info.setExpression(entry.getExpression());
                    }
                    info.setOnline(true);
                    DescribeSubscriptionResponse.ClientSubscriptionInfo.Builder csi =
                        DescribeSubscriptionResponse.ClientSubscriptionInfo.newBuilder()
                            .setClientInfo(buildClientInfo(channel))
                            .setSubscriptionInfo(info);
                    builder.addClientSubscriptionInfo(csi);
                }
            }
            responseObserver.onNext(builder.build());
        } catch (Throwable t) {
            log.warn("describeSubscription failed", t);
            responseObserver.onNext(DescribeSubscriptionResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void deleteSubscription(DeleteSubscriptionRequest request,
        StreamObserver<DeleteSubscriptionResponse> responseObserver) {
        try {
            String topic = request.getTopic().getName();
            String group = request.hasGroup() ? request.getGroup().getName() : "";
            if (topic == null || topic.isEmpty() || group.isEmpty()) {
                responseObserver.onNext(DeleteSubscriptionResponse.newBuilder()
                    .setStatus(fail(Code.BAD_REQUEST, "topic and group are required")).build());
                responseObserver.onCompleted();
                return;
            }
            log.info("deleteSubscription requested for group={}, topic={}", group, topic);
            List<String> brokerAddrs = resolveBrokerAddrs(topic);
            StringBuilder errors = new StringBuilder();
            for (String brokerAddr : brokerAddrs) {
                try {
                    serviceManager.getAdminService().deleteSubscriptionGroup(brokerAddr, group, false,
                        DEFAULT_TIMEOUT_MILLIS);
                } catch (Throwable t) {
                    log.warn("deleteSubscription failed on broker {}", brokerAddr, t);
                    if (errors.length() > 0) {
                        errors.append("; ");
                    }
                    errors.append(brokerAddr).append(": ").append(t.getMessage());
                }
            }
            if (errors.length() > 0) {
                responseObserver.onNext(DeleteSubscriptionResponse.newBuilder()
                    .setStatus(fail(Code.INTERNAL_ERROR, errors.toString())).build());
            } else {
                responseObserver.onNext(DeleteSubscriptionResponse.newBuilder().setStatus(ok()).build());
            }
        } catch (Throwable t) {
            log.warn("deleteSubscription failed", t);
            responseObserver.onNext(DeleteSubscriptionResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void listConsumerConnection(ListConsumerConnectionRequest request,
        StreamObserver<ListConsumerConnectionResponse> responseObserver) {
        try {
            List<ClientInfo> clients = onlineConsumers(request.getGroup());
            responseObserver.onNext(ListConsumerConnectionResponse.newBuilder()
                .setStatus(ok())
                .addAllClientInfo(clients)
                .build());
        } catch (Throwable t) {
            log.warn("listConsumerConnection failed", t);
            responseObserver.onNext(ListConsumerConnectionResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void describeGroupAccumulation(DescribeGroupAccumulationRequest request,
        StreamObserver<DescribeGroupAccumulationResponse> responseObserver) {
        try {
            String group = request.getGroup().getName();
            String topic = request.getTopicsCount() > 0 ? request.getTopics(0).getName() : null;
            List<String> brokerAddrs = resolveBrokerAddrs(topic == null ? group : topic);
            DescribeGroupAccumulationResponse.GroupAccumulation accumulation =
                AdminModelConverter.toGroupAccumulationMultiBroker(serviceManager.getAdminService(), brokerAddrs,
                    group, topic, DEFAULT_TIMEOUT_MILLIS);
            responseObserver.onNext(DescribeGroupAccumulationResponse.newBuilder()
                .setStatus(ok())
                .setAccumulation(accumulation)
                .build());
        } catch (Throwable t) {
            log.warn("describeGroupAccumulation failed", t);
            responseObserver.onNext(DescribeGroupAccumulationResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void resetGroupOffset(ResetGroupOffsetRequest request, StreamObserver<ResetGroupOffsetResponse> responseObserver) {
        try {
            String group = request.getGroup().getName();
            String topic = request.getTopic().getName();
            long resetTimestamp = request.getResetTimestamp().getSeconds() * 1000L;
            // RIP-2 fix: reset on EVERY broker hosting the topic, not just the first one.
            List<String> brokerAddrs = resolveBrokerAddrs(topic);
            StringBuilder errors = new StringBuilder();
            for (String brokerAddr : brokerAddrs) {
                try {
                    serviceManager.getAdminService().resetOffset(brokerAddr, topic, group, resetTimestamp, true,
                        DEFAULT_TIMEOUT_MILLIS);
                } catch (Throwable t) {
                    log.warn("resetGroupOffset failed on broker {}", brokerAddr, t);
                    if (errors.length() > 0) {
                        errors.append("; ");
                    }
                    errors.append(brokerAddr).append(": ").append(t.getMessage());
                }
            }
            if (errors.length() > 0) {
                responseObserver.onNext(ResetGroupOffsetResponse.newBuilder()
                    .setStatus(fail(Code.INTERNAL_ERROR, errors.toString())).build());
            } else {
                responseObserver.onNext(ResetGroupOffsetResponse.newBuilder().setStatus(ok()).build());
            }
        } catch (Throwable t) {
            log.warn("resetGroupOffset failed", t);
            responseObserver.onNext(ResetGroupOffsetResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void queryMessage(ListMessageRequest request, StreamObserver<ListMessageResponse> responseObserver) {
        try {
            if (!request.hasMessageId() && !request.hasMessageKey()) {
                responseObserver.onNext(ListMessageResponse.newBuilder()
                    .setStatus(fail(Code.BAD_REQUEST, "one of message_id or message_key is required"))
                    .build());
                responseObserver.onCompleted();
                return;
            }
            String topic = request.getTopic().getName();
            List<MessageExt> messageExtList = new ArrayList<>();
            if (request.hasMessageId()) {
                String brokerAddr = resolveBrokerAddr(topic);
                messageExtList.add(serviceManager.getAdminService().viewMessage(brokerAddr, topic,
                    decodeOffset(request.getMessageId()), DEFAULT_TIMEOUT_MILLIS));
            } else {
                long begin = request.hasBeginTimestamp() ? request.getBeginTimestamp().getSeconds() * 1000L : 0L;
                long end = request.hasEndTimestamp() ? request.getEndTimestamp().getSeconds() * 1000L :
                    System.currentTimeMillis();
                int maxNums = request.getMaxMessageNums() > 0 ? request.getMaxMessageNums() : 32;
                // RIP-2 fix: a message key may live on any broker hosting the topic; search
                // all of them until the requested number of messages is collected.
                for (String brokerAddr : resolveBrokerAddrs(topic)) {
                    if (messageExtList.size() >= maxNums) {
                        break;
                    }
                    try {
                        messageExtList.addAll(serviceManager.getAdminService().queryMessage(brokerAddr, topic,
                            request.getMessageKey(), maxNums, begin, end,
                            DEFAULT_TIMEOUT_MILLIS));
                    } catch (Throwable t) {
                        log.warn("queryMessage failed on broker {}", brokerAddr, t);
                    }
                }
            }
            ListMessageResponse.Builder builder = ListMessageResponse.newBuilder().setStatus(ok());
            for (MessageExt ext : messageExtList) {
                if (ext == null) {
                    continue;
                }
                builder.addMessages(AdminModelConverter.toMessage(ext));
            }
            responseObserver.onNext(builder.build());
        } catch (Throwable t) {
            log.warn("queryMessage failed", t);
            responseObserver.onNext(ListMessageResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void printThreadStackTrace(PrintThreadStackTraceRequest request,
        StreamObserver<PrintThreadStackTraceResponse> responseObserver) {
        try {
            GrpcClientChannel channel = grpcChannelManager.getChannel(request.getClientId());
            if (channel == null) {
                responseObserver.onNext(PrintThreadStackTraceResponse.newBuilder()
                    .setStatus(fail(Code.NOT_FOUND, "client not connected to this proxy")).build());
            } else {
                channel.writeTelemetryCommand(apache.rocketmq.v2.TelemetryCommand.newBuilder()
                    .setPrintThreadStackTraceCommand(apache.rocketmq.v2.PrintThreadStackTraceCommand.newBuilder().build())
                    .build());
                responseObserver.onNext(PrintThreadStackTraceResponse.newBuilder().setStatus(ok()).build());
            }
        } catch (Throwable t) {
            log.warn("printThreadStackTrace failed", t);
            responseObserver.onNext(PrintThreadStackTraceResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void verifyMessage(VerifyMessageRequest request, StreamObserver<VerifyMessageResponse> responseObserver) {
        try {
            GrpcClientChannel channel = grpcChannelManager.getChannel(request.getClientId());
            if (channel == null) {
                responseObserver.onNext(VerifyMessageResponse.newBuilder()
                    .setStatus(fail(Code.NOT_FOUND, "client not connected to this proxy")).build());
            } else {
                MessageExt ext = new MessageExt();
                ext.setTopic(request.getTopic().getName());
                ext.setMsgId(request.getMessageId());
                ext.setBody(new byte[0]);
                channel.writeTelemetryCommand(apache.rocketmq.v2.TelemetryCommand.newBuilder()
                    .setVerifyMessageCommand(apache.rocketmq.v2.VerifyMessageCommand.newBuilder()
                        .setMessage(org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter.getInstance().buildMessage(ext))
                        .build())
                    .build());
                responseObserver.onNext(VerifyMessageResponse.newBuilder().setStatus(ok()).build());
            }
        } catch (Throwable t) {
            log.warn("verifyMessage failed", t);
            responseObserver.onNext(VerifyMessageResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void adminSendMessage(AdminSendMessageRequest request, StreamObserver<AdminSendMessageResponse> responseObserver) {
        try {
            String topic = request.getTopic().getName();
            org.apache.rocketmq.common.message.Message msg = new org.apache.rocketmq.common.message.Message(
                topic, request.getBody().toByteArray());
            if (request.hasTag()) {
                msg.setTags(request.getTag());
            }
            if (request.hasKey()) {
                msg.setKeys(request.getKey());
            }
            if (request.getUserPropertiesMap() != null) {
                request.getUserPropertiesMap().forEach(msg::putUserProperty);
            }
            List<org.apache.rocketmq.common.message.Message> list = new ArrayList<>();
            list.add(msg);
            List<org.apache.rocketmq.client.producer.SendResult> sendResults =
                messagingProcessor.sendMessage(ctx(), null, "ADMIN_SEND_PRODUCER_GROUP", 0, list,
                    DEFAULT_TIMEOUT_MILLIS).get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            String messageId = sendResults != null && !sendResults.isEmpty() ? sendResults.get(0).getMsgId() : "";
            responseObserver.onNext(AdminSendMessageResponse.newBuilder()
                .setStatus(ok())
                .setMessageId(messageId)
                .build());
        } catch (Throwable t) {
            log.warn("adminSendMessage failed", t);
            responseObserver.onNext(AdminSendMessageResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getConsumerRunningInfo(GetConsumerRunningInfoRequest request,
        StreamObserver<GetConsumerRunningInfoResponse> responseObserver) {
        try {
            GrpcClientChannel channel = grpcChannelManager.getChannel(request.getClientId());
            if (channel == null) {
                responseObserver.onNext(GetConsumerRunningInfoResponse.newBuilder()
                    .setStatus(fail(Code.NOT_FOUND, "client not connected to this proxy")).build());
                responseObserver.onCompleted();
                return;
            }
            apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(request.getClientId());
            ConsumerRunningInfo.Builder cri = ConsumerRunningInfo.newBuilder();
            if (settings != null && settings.hasSubscription()) {
                for (apache.rocketmq.v2.SubscriptionEntry entry : settings.getSubscription().getSubscriptionsList()) {
                    FilterExpression fe = entry.hasExpression() ? entry.getExpression() :
                        FilterExpression.newBuilder().setType(apache.rocketmq.v2.FilterType.TAG).setExpression("*").build();
                    cri.putSubscriptions(entry.hasTopic() ? entry.getTopic().getName() : "", fe);
                }
            }
            responseObserver.onNext(GetConsumerRunningInfoResponse.newBuilder()
                .setStatus(ok())
                .setConsumerRunningInfo(cri)
                .build());
        } catch (Throwable t) {
            log.warn("getConsumerRunningInfo failed", t);
            responseObserver.onNext(GetConsumerRunningInfoResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void queryTimeSpan(QueryTimeSpanRequest request, StreamObserver<QueryTimeSpanResponse> responseObserver) {
        try {
            String group = request.getGroup().getName();
            String topic = request.getTopicsCount() > 0 ? request.getTopics(0).getName() : group;
            String brokerAddr = resolveBrokerAddr(topic);
            org.apache.rocketmq.proxy.service.route.MessageQueueView mqv =
                serviceManager.getTopicRouteService().getAllMessageQueueView(ctx(), topic);
            QueryTimeSpanResponse response = AdminModelConverter.toQueryTimeSpan(
                serviceManager.getAdminService(), brokerAddr, group, topic, mqv, DEFAULT_TIMEOUT_MILLIS);
            responseObserver.onNext(response);
        } catch (Throwable t) {
            log.warn("queryTimeSpan failed", t);
            responseObserver.onNext(QueryTimeSpanResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
        }
        responseObserver.onCompleted();
    }

    // -------------------------------------------------------------------------
    // converters
    // -------------------------------------------------------------------------

    private Resource resource(String name) {
        return Resource.newBuilder().setName(name).build();
    }

    private long decodeOffset(String messageId) {
        try {
            return org.apache.rocketmq.common.message.MessageDecoder.decodeMessageId(messageId).getOffset();
        } catch (Throwable t) {
            return 0L;
        }
    }
}
