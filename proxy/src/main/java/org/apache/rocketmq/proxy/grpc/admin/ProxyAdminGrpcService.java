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
import apache.rocketmq.v2.AdminSendMessageRequest;
import apache.rocketmq.v2.AdminSendMessageResponse;
import apache.rocketmq.v2.ChangeLogLevelRequest;
import apache.rocketmq.v2.ChangeLogLevelResponse;
import apache.rocketmq.v2.ClientInfo;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ConsumerRunningInfo;
import apache.rocketmq.v2.DeleteSubscriptionRequest;
import apache.rocketmq.v2.DeleteSubscriptionResponse;
import apache.rocketmq.v2.DescribeGroupAccumulationRequest;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse;
import apache.rocketmq.v2.DescribeSubscriptionRequest;
import apache.rocketmq.v2.DescribeSubscriptionResponse;
import apache.rocketmq.v2.DescribeTopicStatusRequest;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
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
import apache.rocketmq.v2.ResetGroupOffsetRequest;
import apache.rocketmq.v2.ResetGroupOffsetResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SubscriptionInfo;
import apache.rocketmq.v2.SystemProperties;
import apache.rocketmq.v2.VerifyMessageRequest;
import apache.rocketmq.v2.VerifyMessageResponse;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayRequest;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * Proxy Admin gRPC service (control plane).
 *
 * <p>Design rules this class follows:
 * <ul>
 *   <li><b>Never block the gRPC executor.</b> Every broker hop goes through the asynchronous
 *       {@link AdminService} gateway and is fanned out to all relevant brokers concurrently.</li>
 *   <li><b>Cluster-wide view.</b> Subscription and connection data is read from the brokers (which
 *       see consumers registered through every proxy) rather than from this proxy's local channel
 *       table, and consumers owned by a peer proxy are reached by forwarding the whole RPC.</li>
 *   <li><b>Honest responses.</b> A field the open-source proxy genuinely cannot supply is left
 *       unset and explained in the status message, instead of being filled with a plausible value.</li>
 *   <li><b>Graded errors.</b> Failures are mapped through {@link ResponseBuilder#buildStatus(Throwable)}
 *       so callers can tell "topic not found" from "internal error".</li>
 * </ul>
 *
 * <p>The translation between broker wire types and the v2 contract lives in
 * {@link AdminModelConverter}; whole-message conversion reuses the data plane's
 * {@link GrpcConverter} instead of duplicating it.
 */
public class ProxyAdminGrpcService extends AdminGrpc.AdminImplBase implements StartAndShutdown {

    private static final Logger log = LoggerFactory.getLogger(ProxyAdminGrpcService.class);

    /**
     * Bounds every wait this service performs that is not already bounded by the callee: the answer
     * to a relayed telemetry command, and each broker hop of a fan-out. The gRPC nonce sweeper and
     * the remoting timeout cover the healthy paths, but a hop that never completes at all (an
     * unwritable channel, a saturated blocking-call pool) would otherwise leave the RPC hanging
     * forever with no response at all.
     */
    private final ScheduledExecutorService adminTimeoutScheduler =
        ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ProxyAdminTimeout_", true));

    /**
     * How much longer than the broker timeout a single hop is allowed to take. The broker call
     * carries its own timeout, so this only catches a gateway future that never completes; the slack
     * keeps a genuine broker timeout reporting its own cause instead of this generic deadline.
     */
    private static final long BROKER_CALL_DEADLINE_SLACK_MILLIS = 1000L;

    private static final String ADMIN_SEND_PRODUCER_GROUP = "ADMIN_SEND_PRODUCER_GROUP";
    private static final int DEFAULT_MAX_MESSAGE_NUMS = 32;

    private final ServiceManager serviceManager;
    private final MessagingProcessor messagingProcessor;
    private final GrpcChannelManager grpcChannelManager;
    private final GrpcClientSettingsManager grpcClientSettingsManager;
    private final ProxyAdminForwarder forwarder;

    public ProxyAdminGrpcService(ServiceManager serviceManager, MessagingProcessor messagingProcessor,
        GrpcChannelManager grpcChannelManager, GrpcClientSettingsManager grpcClientSettingsManager,
        ProxyAdminForwarder forwarder) {
        this.serviceManager = serviceManager;
        this.messagingProcessor = messagingProcessor;
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.forwarder = forwarder;
    }

    // =========================================================================
    // helpers
    // =========================================================================

    private AdminService admin() {
        return this.serviceManager.getAdminService();
    }

    private long timeoutMillis() {
        return ConfigurationManager.getProxyConfig().getGrpcAdminServerRequestTimeoutMillis();
    }

    private ProxyContext ctx() {
        return ProxyContext.create();
    }

    private static Status ok() {
        return AdminModelConverter.ok();
    }

    private static Status err(Throwable t) {
        return ResponseBuilder.getInstance().buildStatus(t);
    }

    private static Status err(Code code, String message) {
        return ResponseBuilder.getInstance().buildStatus(code, message);
    }

    /** Sends exactly one response. Splitting onNext/onCompleted across a try and a catch is what
     *  makes a StreamObserver throw IllegalStateException on a partially-succeeded call. */
    private static <T> void respond(StreamObserver<T> observer, T response) {
        observer.onNext(response);
        observer.onCompleted();
    }

    /** Master broker address of every broker group in the cluster. */
    private CompletableFuture<List<String>> allMasterBrokerAddrs() {
        return admin().getBrokerClusterInfo(timeoutMillis()).thenApply(clusterInfo -> {
            List<String> addrs = new ArrayList<>();
            if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
                return addrs;
            }
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                String master = brokerData.getBrokerAddrs() == null ? null
                    : brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                if (master != null && !master.isEmpty()) {
                    addrs.add(master);
                }
            }
            return addrs;
        });
    }

    /**
     * Addresses of the brokers hosting a topic. Write operations must use the write selector: the
     * read selector can omit brokers that only accept writes and vice versa.
     */
    private List<String> topicBrokerAddrs(String topic, boolean forWrite) throws Exception {
        MessageQueueView view = serviceManager.getTopicRouteService().getAllMessageQueueView(ctx(), topic);
        if (view == null) {
            throw new IllegalStateException("topic route not found for " + topic);
        }
        List<AddressableMessageQueue> queues = forWrite
            ? view.getWriteSelector().getQueues() : view.getReadSelector().getQueues();
        if (queues == null || queues.isEmpty()) {
            throw new IllegalStateException("no " + (forWrite ? "writable" : "readable") + " queue for topic " + topic);
        }
        Set<String> addrs = new LinkedHashSet<>();
        for (AddressableMessageQueue queue : queues) {
            if (queue.getBrokerAddr() != null && !queue.getBrokerAddr().isEmpty()) {
                addrs.add(queue.getBrokerAddr());
            }
        }
        if (addrs.isEmpty()) {
            throw new IllegalStateException("broker address not found for topic " + topic);
        }
        return new ArrayList<>(addrs);
    }

    /**
     * Runs a query against every broker concurrently and keeps the successful answers. A broker
     * that fails does not fail the whole call — a partially available cluster should still return
     * the data it has. Only when every broker fails is the first error propagated, so the caller
     * reports a real cause instead of an empty success.
     */
    private <T> CompletableFuture<Map<String, T>> queryAllBrokers(Collection<String> brokerAddrs,
        BiFunction<String, Long, CompletableFuture<T>> query) {
        if (brokerAddrs == null || brokerAddrs.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        long timeout = timeoutMillis();
        Map<String, CompletableFuture<T>> pending = new LinkedHashMap<>();
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        for (String brokerAddr : brokerAddrs) {
            CompletableFuture<T> hop = withTimeout(query.apply(brokerAddr, timeout),
                timeout + BROKER_CALL_DEADLINE_SLACK_MILLIS, "broker call to " + brokerAddr);
            pending.put(brokerAddr, hop
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        failures.add(throwable);
                    }
                }));
        }
        return CompletableFuture.allOf(pending.values().toArray(new CompletableFuture[0]))
            .handle((ignored, throwable) -> {
                Map<String, T> succeeded = new LinkedHashMap<>();
                for (Map.Entry<String, CompletableFuture<T>> entry : pending.entrySet()) {
                    CompletableFuture<T> future = entry.getValue();
                    if (!future.isCompletedExceptionally()) {
                        T result = future.getNow(null);
                        if (result != null) {
                            succeeded.put(entry.getKey(), result);
                        }
                    }
                }
                return succeeded;
            })
            .thenCompose(succeeded -> {
                if (succeeded.isEmpty() && !failures.isEmpty()) {
                    CompletableFuture<Map<String, T>> failed = new CompletableFuture<>();
                    failed.completeExceptionally(failures.get(0));
                    return failed;
                }
                return CompletableFuture.completedFuture(succeeded);
            });
    }

    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, long timeoutMillis,
        String what) {
        CompletableFuture<T> timeout = new CompletableFuture<>();
        ScheduledFuture<?> timer = this.adminTimeoutScheduler.schedule(
            () -> timeout.completeExceptionally(new IllegalStateException(
                what + " timed out after " + timeoutMillis + "ms")),
            timeoutMillis, TimeUnit.MILLISECONDS);
        // one admin RPC fans out to every broker, so the timer is cancelled as soon as the answer
        // arrives instead of leaving a dead task per hop queued until its deadline
        return future.applyToEither(timeout, result -> result)
            .whenComplete((result, throwable) -> timer.cancel(false));
    }

    private static Resource resource(String name) {
        return Resource.newBuilder().setName(name == null ? "" : name).build();
    }

    /**
     * Locates the client a client-targeted RPC is about. Returns the channel info when the client
     * is known to this proxy's consumer manager (which, in cluster mode, also holds entries synced
     * from peer proxies), or null.
     */
    private ClientChannelInfo findClientChannel(String group, String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return null;
        }
        ConsumerManager consumerManager = serviceManager.getConsumerManager();
        if (consumerManager == null) {
            return null;
        }
        if (group != null && !group.isEmpty()) {
            ClientChannelInfo info = consumerManager.findChannel(group, clientId);
            if (info != null) {
                return info;
            }
        }
        // the caller may not know the group; the local gRPC channel table is keyed by clientId alone
        GrpcClientChannel channel = grpcChannelManager.getChannel(clientId);
        if (channel == null) {
            return null;
        }
        return new ClientChannelInfo(channel, clientId, null, 0);
    }

    // =========================================================================
    // 1. ChangeLogLevel
    // =========================================================================

    @Override
    public void changeLogLevel(ChangeLogLevelRequest request, StreamObserver<ChangeLogLevelResponse> responseObserver) {
        String remark;
        try {
            org.apache.rocketmq.logging.org.slf4j.ILoggerFactory factory =
                org.apache.rocketmq.logging.org.slf4j.LoggerFactory.getILoggerFactory();
            if (!(factory instanceof org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext)) {
                remark = "unsupported logging backend, cannot change log level at runtime";
            } else {
                ChangeLogLevelRequest.Level level = request.getLevel();
                // Level.TRACE is enum value 0, so proto3 cannot distinguish "caller asked for TRACE"
                // from "caller sent an empty request". Honour the contract but make the consequence
                // visible instead of silently switching a production proxy to the most verbose level.
                if (level == ChangeLogLevelRequest.Level.TRACE) {
                    log.warn("changeLogLevel requested TRACE, which is also the proto3 default for an "
                        + "unset level; make sure this is intentional");
                }
                org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext loggerContext =
                    (org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext) factory;
                org.apache.rocketmq.logging.ch.qos.logback.classic.Level logbackLevel =
                    org.apache.rocketmq.logging.ch.qos.logback.classic.Level.toLevel(level.name());
                loggerContext.getLogger(
                        org.apache.rocketmq.logging.ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME)
                    .setLevel(logbackLevel);
                remark = "log level changed to " + logbackLevel;
            }
        } catch (Throwable t) {
            log.warn("changeLogLevel failed", t);
            remark = "failed to change log level: " + t.getMessage();
        }
        respond(responseObserver, ChangeLogLevelResponse.newBuilder().setRemark(remark).build());
    }

    // =========================================================================
    // 2. DescribeTopicStatus
    // =========================================================================

    @Override
    public void describeTopicStatus(DescribeTopicStatusRequest request,
        StreamObserver<DescribeTopicStatusResponse> responseObserver) {
        String topic = request.getTopic().getName();
        List<String> brokerAddrs;
        try {
            brokerAddrs = topicBrokerAddrs(topic, false);
        } catch (Throwable t) {
            respond(responseObserver, DescribeTopicStatusResponse.newBuilder().setStatus(err(t)).build());
            return;
        }
        // a topic is normally configured identically on every broker hosting it, but asking only the
        // first one hides a real inconsistency, so all of them are queried and compared
        queryAllBrokers(brokerAddrs, (addr, timeout) -> admin().getTopicConfig(addr, topic, timeout))
            .whenComplete((configs, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, DescribeTopicStatusResponse.newBuilder().setStatus(err(throwable)).build());
                    return;
                }
                try {
                    respond(responseObserver, AdminModelConverter.toTopicStatus(
                        mergeTopicConfigs(configs.values()), topic));
                } catch (Throwable t) {
                    respond(responseObserver, DescribeTopicStatusResponse.newBuilder().setStatus(err(t)).build());
                }
            });
    }

    private static TopicConfig mergeTopicConfigs(Collection<TopicConfig> configs) {
        TopicConfig merged = null;
        TopicMessageType messageType = null;
        int readQueueNums = 0;
        int writeQueueNums = 0;
        boolean consistent = true;
        for (TopicConfig config : configs) {
            if (config == null) {
                continue;
            }
            if (merged == null) {
                merged = config;
                messageType = config.getTopicMessageType();
            } else if (messageType != config.getTopicMessageType()) {
                consistent = false;
            }
            readQueueNums += config.getReadQueueNums();
            writeQueueNums += config.getWriteQueueNums();
        }
        if (merged == null) {
            return null;
        }
        TopicConfig result = new TopicConfig();
        result.setTopicName(merged.getTopicName());
        result.setReadQueueNums(readQueueNums);
        result.setWriteQueueNums(writeQueueNums);
        result.setPerm(merged.getPerm());
        // MIXED is the honest answer when brokers disagree, and TopicConfig maps it back to
        // MESSAGE_TYPE_UNSPECIFIED in the response rather than pretending one broker is right
        result.setAttributes(new HashMap<>());
        result.getAttributes().put(org.apache.rocketmq.common.TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE.getName(),
            (consistent ? messageType : TopicMessageType.MIXED).name());
        return result;
    }

    // =========================================================================
    // 3. ListSubscription
    // =========================================================================

    @Override
    public void listSubscription(ListSubscriptionRequest request, StreamObserver<ListSubscriptionResponse> responseObserver) {
        boolean hasTopic = request.hasTopic() && !request.getTopic().getName().isEmpty();
        boolean hasGroup = request.hasGroup() && !request.getGroup().getName().isEmpty();
        if (!hasTopic && !hasGroup) {
            respond(responseObserver, ListSubscriptionResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "at least one of topic or group must be set")).build());
            return;
        }
        String topic = hasTopic ? request.getTopic().getName() : null;
        String group = hasGroup ? request.getGroup().getName() : null;
        resolveGroups(topic, group)
            .thenCompose(groups -> allMasterBrokerAddrs()
                .thenCompose(brokers -> collectConnections(brokers, groups)))
            .whenComplete((connections, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, ListSubscriptionResponse.newBuilder().setStatus(err(throwable)).build());
                    return;
                }
                try {
                    respond(responseObserver, buildListSubscriptionResponse(connections, topic));
                } catch (Throwable t) {
                    respond(responseObserver, ListSubscriptionResponse.newBuilder().setStatus(err(t)).build());
                }
            });
    }

    private ListSubscriptionResponse buildListSubscriptionResponse(Map<String, ConsumerConnection> connections,
        String topicFilter) {
        ListSubscriptionResponse.Builder builder = ListSubscriptionResponse.newBuilder().setStatus(ok());
        // a group reported by several brokers, or by several clients, describes the same
        // subscription; keyed by group+topic+expression so the answer does not scale with
        // the number of online consumers
        Map<String, SubscriptionInfo> dedup = new LinkedHashMap<>();
        String pullRetryPrefix = MixAll.RETRY_GROUP_TOPIC_PREFIX;
        for (Map.Entry<String, ConsumerConnection> entry : connections.entrySet()) {
            String group = entry.getKey();
            ConsumerConnection connection = entry.getValue();
            if (connection == null || connection.getSubscriptionTable() == null) {
                continue;
            }
            boolean online = connection.getConnectionSet() != null && !connection.getConnectionSet().isEmpty();
            apache.rocketmq.v2.MessageModel messageModel = AdminModelConverter.toMessageModel(connection.getMessageModel());
            for (Map.Entry<String, org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData> sub :
                connection.getSubscriptionTable().entrySet()) {
                String subTopic = sub.getKey();
                if (subTopic == null || subTopic.startsWith(pullRetryPrefix) || KeyBuilder.isPopRetryTopicV2(subTopic)) {
                    continue;
                }
                if (topicFilter != null && !topicFilter.equals(subTopic)) {
                    continue;
                }
                org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData data = sub.getValue();
                SubscriptionInfo info = SubscriptionInfo.newBuilder()
                    .setGroup(resource(group))
                    .setTopic(resource(subTopic))
                    .setExpression(AdminModelConverter.toFilterExpression(
                        data == null ? null : data.getExpressionType(), data == null ? null : data.getSubString()))
                    .setOnline(online)
                    .setMessageModel(messageModel)
                    .build();
                dedup.put(group + "\u0001" + subTopic, info);
            }
        }
        for (SubscriptionInfo info : dedup.values()) {
            builder.addSubscriptionInfo(info);
        }
        return builder.build();
    }

    /** Groups to inspect: the requested one, or every group consuming the requested topic. */
    private CompletableFuture<Set<String>> resolveGroups(String topic, String group) {
        if (group != null) {
            return CompletableFuture.completedFuture(Collections.singleton(group));
        }
        return allMasterBrokerAddrs().thenCompose(brokers ->
            queryAllBrokers(brokers, (addr, timeout) -> admin().queryTopicConsumeByWho(addr, topic, timeout))
                .thenApply(byBroker -> {
                    Set<String> groups = new LinkedHashSet<>();
                    for (GroupList groupList : byBroker.values()) {
                        if (groupList != null && groupList.getGroupList() != null) {
                            groups.addAll(groupList.getGroupList());
                        }
                    }
                    return groups;
                }));
    }

    /** Consumer connections of each group, merged across brokers. */
    private CompletableFuture<Map<String, ConsumerConnection>> collectConnections(List<String> brokers,
        Collection<String> groups) {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        Map<String, ConsumerConnection> merged = new LinkedHashMap<>();
        for (String group : groups) {
            tasks.add(queryAllBrokers(brokers,
                (addr, timeout) -> admin().getConsumerConnectionList(addr, group, timeout))
                .handle((byBroker, throwable) -> {
                    // seed with the proxy-side view: gRPC v2 consumers never reach the broker, and a
                    // broker that does not know the group must not erase what this proxy does know
                    ConsumerConnection acc = proxySideConnection(group);
                    if (throwable != null) {
                        log.info("broker has no consumer connection for group {}: {}", group, throwable.getMessage());
                    } else {
                        for (ConsumerConnection connection : byBroker.values()) {
                            acc = mergeConsumerConnection(acc, connection);
                        }
                    }
                    if (acc != null) {
                        synchronized (merged) {
                            merged.put(group, acc);
                        }
                    }
                    return null;
                }));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
            .thenApply(ignored -> merged);
    }

    /**
     * Consumers registered through this proxy cluster, synthesised into the same shape the broker
     * reports so the two sources can be merged without the callers knowing where a client came from.
     *
     * <p>A gRPC v2 client keeps its channel on the proxy and is never registered in the broker's
     * consumer manager, so a broker-only query returns nothing for it. {@code HeartbeatSyncer}
     * replicates registrations between proxies, so this view also covers clients connected to a peer
     * proxy (they appear as {@link org.apache.rocketmq.proxy.processor.channel.RemoteChannel}).
     */
    private ConsumerConnection proxySideConnection(String group) {
        ConsumerManager consumerManager = serviceManager.getConsumerManager();
        ConsumerGroupInfo groupInfo = consumerManager == null ? null : consumerManager.getConsumerGroupInfo(group);
        if (groupInfo == null) {
            return null;
        }
        ConsumerConnection connection = new ConsumerConnection();
        connection.setConsumeType(groupInfo.getConsumeType());
        connection.setMessageModel(groupInfo.getMessageModel());
        connection.setConsumeFromWhere(groupInfo.getConsumeFromWhere());
        if (groupInfo.getSubscriptionTable() != null) {
            connection.getSubscriptionTable().putAll(groupInfo.getSubscriptionTable());
        }
        for (ClientChannelInfo channelInfo : groupInfo.getChannelInfoTable().values()) {
            if (channelInfo == null) {
                continue;
            }
            Connection client = new Connection();
            client.setClientId(channelInfo.getClientId());
            client.setLanguage(channelInfo.getLanguage());
            client.setVersion(channelInfo.getVersion());
            client.setClientAddr(remoteAddressOf(channelInfo));
            connection.getConnectionSet().add(client);
        }
        return connection;
    }

    /** Netty renders a socket address as "/ip:port"; drop the leading slash so it parses as an IP. */
    private static String remoteAddressOf(ClientChannelInfo channelInfo) {
        if (channelInfo.getChannel() == null || channelInfo.getChannel().remoteAddress() == null) {
            return "";
        }
        String address = channelInfo.getChannel().remoteAddress().toString();
        return address.startsWith("/") ? address.substring(1) : address;
    }

    private static ConsumerConnection mergeConsumerConnection(ConsumerConnection target, ConsumerConnection source) {
        if (source == null) {
            return target;
        }
        if (target == null) {
            ConsumerConnection copy = new ConsumerConnection();
            copy.setConsumeType(source.getConsumeType());
            copy.setMessageModel(source.getMessageModel());
            copy.setConsumeFromWhere(source.getConsumeFromWhere());
            copy.getConnectionSet().addAll(source.getConnectionSet());
            copy.getSubscriptionTable().putAll(source.getSubscriptionTable());
            return copy;
        }
        for (Connection connection : source.getConnectionSet()) {
            boolean seen = false;
            for (Connection existing : target.getConnectionSet()) {
                if (existing.getClientId() != null && existing.getClientId().equals(connection.getClientId())) {
                    seen = true;
                    break;
                }
            }
            if (!seen) {
                target.getConnectionSet().add(connection);
            }
        }
        target.getSubscriptionTable().putAll(source.getSubscriptionTable());
        return target;
    }

    // =========================================================================
    // 4. DescribeSubscription
    // =========================================================================

    @Override
    public void describeSubscription(DescribeSubscriptionRequest request,
        StreamObserver<DescribeSubscriptionResponse> responseObserver) {
        boolean hasTopic = request.hasTopic() && !request.getTopic().getName().isEmpty();
        boolean hasGroup = request.hasGroup() && !request.getGroup().getName().isEmpty();
        if (!hasTopic && !hasGroup) {
            respond(responseObserver, DescribeSubscriptionResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "at least one of topic or group must be set")).build());
            return;
        }
        String topic = hasTopic ? request.getTopic().getName() : null;
        String group = hasGroup ? request.getGroup().getName() : null;
        resolveGroups(topic, group)
            .thenCompose(groups -> allMasterBrokerAddrs()
                .thenCompose(brokers -> collectConnections(brokers, groups)))
            .whenComplete((connections, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, DescribeSubscriptionResponse.newBuilder().setStatus(err(throwable)).build());
                    return;
                }
                try {
                    respond(responseObserver, buildDescribeSubscriptionResponse(connections, topic));
                } catch (Throwable t) {
                    respond(responseObserver, DescribeSubscriptionResponse.newBuilder().setStatus(err(t)).build());
                }
            });
    }

    /**
     * One entry per connected client, which is what makes inconsistent subscriptions inside a group
     * visible. Per-client settings are only held for clients connected to this proxy, so a client
     * owned by a peer proxy is reported from the broker-side connection data with the group-level
     * subscription rather than being dropped.
     */
    private DescribeSubscriptionResponse buildDescribeSubscriptionResponse(Map<String, ConsumerConnection> connections,
        String topicFilter) {
        DescribeSubscriptionResponse.Builder builder = DescribeSubscriptionResponse.newBuilder().setStatus(ok());
        for (Map.Entry<String, ConsumerConnection> entry : connections.entrySet()) {
            String group = entry.getKey();
            ConsumerConnection connection = entry.getValue();
            if (connection == null) {
                continue;
            }
            apache.rocketmq.v2.MessageModel messageModel = AdminModelConverter.toMessageModel(connection.getMessageModel());
            for (Connection conn : connection.getConnectionSet()) {
                ClientInfo clientInfo = AdminModelConverter.toClientInfo(conn, messageModel);
                apache.rocketmq.v2.Settings settings =
                    grpcClientSettingsManager.getRawClientSettings(conn.getClientId());
                List<SubscriptionInfo> infos = settings != null && settings.hasSubscription()
                    ? subscriptionsFromSettings(group, settings, topicFilter, messageModel)
                    : subscriptionsFromConnection(group, connection, topicFilter);
                for (SubscriptionInfo info : infos) {
                    builder.addClientSubscriptionInfo(
                        DescribeSubscriptionResponse.ClientSubscriptionInfo.newBuilder()
                            .setClientInfo(clientInfo)
                            .setSubscriptionInfo(info)
                            .build());
                }
            }
        }
        return builder.build();
    }

    private List<SubscriptionInfo> subscriptionsFromSettings(String group, apache.rocketmq.v2.Settings settings,
        String topicFilter, apache.rocketmq.v2.MessageModel messageModel) {
        List<SubscriptionInfo> result = new ArrayList<>();
        for (apache.rocketmq.v2.SubscriptionEntry entry : settings.getSubscription().getSubscriptionsList()) {
            String entryTopic = entry.hasTopic() ? entry.getTopic().getName() : "";
            if (topicFilter != null && !topicFilter.equals(entryTopic)) {
                continue;
            }
            SubscriptionInfo.Builder info = SubscriptionInfo.newBuilder()
                .setGroup(resource(group))
                .setTopic(resource(entryTopic))
                .setOnline(true)
                .setMessageModel(messageModel);
            if (entry.hasExpression()) {
                info.setExpression(entry.getExpression());
            }
            result.add(info.build());
        }
        return result;
    }

    private List<SubscriptionInfo> subscriptionsFromConnection(String group, ConsumerConnection connection,
        String topicFilter) {
        List<SubscriptionInfo> result = new ArrayList<>();
        if (connection.getSubscriptionTable() == null) {
            return result;
        }
        apache.rocketmq.v2.MessageModel messageModel = AdminModelConverter.toMessageModel(connection.getMessageModel());
        for (Map.Entry<String, org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData> sub :
            connection.getSubscriptionTable().entrySet()) {
            if (topicFilter != null && !topicFilter.equals(sub.getKey())) {
                continue;
            }
            org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData data = sub.getValue();
            result.add(SubscriptionInfo.newBuilder()
                .setGroup(resource(group))
                .setTopic(resource(sub.getKey()))
                .setExpression(AdminModelConverter.toFilterExpression(
                    data == null ? null : data.getExpressionType(), data == null ? null : data.getSubString()))
                .setOnline(true)
                .setMessageModel(messageModel)
                .build());
        }
        return result;
    }

    // =========================================================================
    // 5. DeleteSubscription
    // =========================================================================

    /**
     * Deletes ONE subscription relationship (group + topic + filter expression), which is what the
     * contract asks for. It deliberately does NOT delete the consumer group: an earlier version
     * answered this request with {@code deleteSubscriptionGroup} on every broker hosting the topic,
     * which destroyed the group's other subscriptions along with its offsets.
     *
     * <p>The broker has no request code for a granular delete, so this is a read-modify-write on
     * {@link SubscriptionGroupConfig#getSubscriptionDataSet()} via UPDATE_AND_CREATE_SUBSCRIPTIONGROUP.
     *
     * <p><b>In open source this is effectively a no-op, and that is expected rather than a bug.</b>
     * Nothing in the codebase ever writes {@code subscriptionDataSet}: the broker only reads it
     * ({@code ConsumerLagCalculator}, to attribute lag to topics), so on a stock deployment the set
     * is {@code null} and there is no persisted relationship to remove. Durable per-relationship
     * subscriptions are a downstream concern: distributions that need this RPC back it with an
     * external subscription store that populates the same field. Live subscriptions held in the
     * broker's {@code ConsumerManager} are deliberately not touched: they are rebuilt from client
     * heartbeats, so removing them there would be undone within one heartbeat interval and would
     * only look like a successful delete.
     *
     * <p>Callers therefore observe {@link Code#NOT_FOUND} with an explanation, and the consumer group
     * and its offsets are left untouched. Making this RPC functional requires a persisted
     * subscription store (or a contract change), which is out of scope for the proxy.
     */
    @Override
    public void deleteSubscription(DeleteSubscriptionRequest request,
        StreamObserver<DeleteSubscriptionResponse> responseObserver) {
        String topic = request.getTopic().getName();
        String group = request.hasGroup() ? request.getGroup().getName() : "";
        if (topic.isEmpty() || group.isEmpty()) {
            respond(responseObserver, DeleteSubscriptionResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "topic and group are required")).build());
            return;
        }
        boolean hasExpression = request.hasExpression() && !request.getExpression().getExpression().isEmpty();
        String expression = hasExpression ? request.getExpression().getExpression() : null;
        boolean sql = hasExpression && request.getExpression().getType() == apache.rocketmq.v2.FilterType.SQL;
        List<String> brokerAddrs;
        try {
            brokerAddrs = topicBrokerAddrs(topic, true);
        } catch (Throwable t) {
            respond(responseObserver, DeleteSubscriptionResponse.newBuilder().setStatus(err(t)).build());
            return;
        }
        log.info("deleteSubscription group={} topic={} expression={} brokers={}", group, topic, expression, brokerAddrs);

        long timeout = timeoutMillis();
        List<CompletableFuture<Boolean>> tasks = new ArrayList<>();
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        for (String brokerAddr : brokerAddrs) {
            tasks.add(admin().getSubscriptionGroupConfig(brokerAddr, group, timeout)
                .thenCompose(config -> {
                    if (config == null) {
                        return CompletableFuture.completedFuture(false);
                    }
                    Set<SimpleSubscriptionData> dataSet = config.getSubscriptionDataSet();
                    if (dataSet == null || dataSet.isEmpty()) {
                        // the normal path on a stock deployment: nothing persists relationships here,
                        // so this falls through to the NOT_FOUND explained in the method javadoc
                        return CompletableFuture.completedFuture(false);
                    }
                    Set<SimpleSubscriptionData> remaining = new HashSet<>();
                    boolean removed = false;
                    for (SimpleSubscriptionData data : dataSet) {
                        if (matches(data, topic, expression, sql)) {
                            removed = true;
                        } else {
                            remaining.add(data);
                        }
                    }
                    if (!removed) {
                        return CompletableFuture.completedFuture(false);
                    }
                    config.setSubscriptionDataSet(remaining);
                    return admin().updateSubscriptionGroupConfig(brokerAddr, config, timeout).thenApply(v -> true);
                })
                .exceptionally(t -> {
                    failures.add(t);
                    log.warn("deleteSubscription failed on broker {} group={} topic={}", brokerAddr, group, topic, t);
                    return false;
                }));
        }
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).whenComplete((ignored, throwable) -> {
            boolean anyRemoved = false;
            for (CompletableFuture<Boolean> task : tasks) {
                if (Boolean.TRUE.equals(task.getNow(false))) {
                    anyRemoved = true;
                }
            }
            if (anyRemoved) {
                respond(responseObserver, DeleteSubscriptionResponse.newBuilder().setStatus(ok()).build());
            } else if (!failures.isEmpty()) {
                respond(responseObserver, DeleteSubscriptionResponse.newBuilder().setStatus(err(failures.get(0))).build());
            } else {
                // Nothing matched. Say why precisely instead of reporting success for a no-op, and
                // make clear the consumer group itself was left alone: the previous implementation
                // answered this request by deleting the whole group on every broker.
                respond(responseObserver, DeleteSubscriptionResponse.newBuilder()
                    .setStatus(err(Code.NOT_FOUND,
                        "no subscription of group " + group + " on topic " + topic
                            + " is recorded on any broker. Open-source RocketMQ does not persist "
                            + "per-topic subscription relationships (SubscriptionGroupConfig."
                            + "subscriptionDataSet is only ever read by the broker, so it stays empty "
                            + "unless an external subscription store writes it), so there is nothing to "
                            + "delete. The consumer group and its offsets were left untouched."))
                    .build());
            }
        });
    }

    private static boolean matches(SimpleSubscriptionData data, String topic, String expression, boolean sql) {
        if (data == null || !topic.equals(data.getTopic())) {
            return false;
        }
        if (expression == null) {
            return true;
        }
        if (!expression.equals(data.getExpression())) {
            return false;
        }
        String type = sql ? org.apache.rocketmq.common.filter.ExpressionType.SQL92
            : org.apache.rocketmq.common.filter.ExpressionType.TAG;
        return data.getExpressionType() == null || type.equals(data.getExpressionType());
    }

    // =========================================================================
    // 6. DescribeGroupAccumulation
    // =========================================================================

    @Override
    public void describeGroupAccumulation(DescribeGroupAccumulationRequest request,
        StreamObserver<DescribeGroupAccumulationResponse> responseObserver) {
        String group = request.getGroup().getName();
        if (group.isEmpty()) {
            respond(responseObserver, DescribeGroupAccumulationResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "group is required")).build());
            return;
        }
        Set<String> requested = new LinkedHashSet<>();
        for (Resource resource : request.getTopicsList()) {
            if (!resource.getName().isEmpty()) {
                requested.add(resource.getName());
            }
        }
        // The contract says an empty topic list means "the whole group". Resolving that by treating
        // the group name as a topic name cannot work, so the topics the group actually consumes are
        // asked from the brokers instead.
        CompletableFuture<Set<String>> topics = requested.isEmpty()
            ? resolveTopicsOfGroup(group)
            : CompletableFuture.completedFuture(requested);

        topics.thenCompose(topicSet -> allMasterBrokerAddrs()
                .thenCompose(brokers -> queryAllBrokers(brokers,
                    (addr, timeout) -> admin().getConsumeStats(addr, group, "", timeout)))
                .thenApply(byBroker -> AdminModelConverter.toAccumulation(
                    filterToTopics(byBroker, group, topicSet), group)))
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, DescribeGroupAccumulationResponse.newBuilder()
                        .setStatus(err(throwable)).build());
                    return;
                }
                try {
                    DescribeGroupAccumulationResponse.Builder builder = DescribeGroupAccumulationResponse.newBuilder()
                        .setStatus(ok())
                        .setAccumulation(result.total);
                    for (Map.Entry<String, apache.rocketmq.v2.DescribeGroupAccumulationResponse.GroupAccumulation> e :
                        result.byTopic.entrySet()) {
                        builder.putTopicAccumulation(e.getKey(), e.getValue());
                    }
                    respond(responseObserver, builder.build());
                } catch (Throwable t) {
                    respond(responseObserver, DescribeGroupAccumulationResponse.newBuilder().setStatus(err(t)).build());
                }
            });
    }

    /** Topics a group consumes, including its pop retry topics, asked from the brokers. */
    private CompletableFuture<Set<String>> resolveTopicsOfGroup(String group) {
        return allMasterBrokerAddrs().thenCompose(brokers ->
            queryAllBrokers(brokers, (addr, timeout) -> admin().queryTopicsByConsumer(addr, group, timeout))
                .thenApply(byBroker -> {
                    Set<String> topics = new LinkedHashSet<>();
                    for (TopicList topicList : byBroker.values()) {
                        if (topicList == null || topicList.getTopicList() == null) {
                            continue;
                        }
                        for (String topic : topicList.getTopicList()) {
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                                continue;
                            }
                            topics.add(topic);
                        }
                    }
                    return topics;
                }));
    }

    /**
     * Keeps only the queues belonging to the requested topics. When the caller asked for the whole
     * group everything is kept, so a caller scoping the query to one topic does not get the backlog
     * of unrelated topics mixed in.
     */
    private Map<String, ConsumeStats> filterToTopics(Map<String, ConsumeStats> byBroker, String group,
        Set<String> requestedTopics) {
        if (requestedTopics.isEmpty()) {
            return byBroker;
        }
        Set<String> wanted = new HashSet<>();
        for (String topic : requestedTopics) {
            wanted.add(topic);
            // a topic's backlog is spread over its own queues and the group's retry queues for it
            wanted.add(KeyBuilder.buildPopRetryTopic(topic, group));
            wanted.add(KeyBuilder.buildPopRetryTopicV2(topic, group));
        }
        wanted.add(MixAll.getRetryTopic(group));
        Map<String, ConsumeStats> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, ConsumeStats> entry : byBroker.entrySet()) {
            ConsumeStats stats = entry.getValue();
            if (stats == null || stats.getOffsetTable() == null) {
                continue;
            }
            ConsumeStats copy = new ConsumeStats();
            for (Map.Entry<MessageQueue, org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper> e :
                stats.getOffsetTable().entrySet()) {
                if (e.getKey() != null && wanted.contains(e.getKey().getTopic())) {
                    copy.getOffsetTable().put(e.getKey(), e.getValue());
                }
            }
            filtered.put(entry.getKey(), copy);
        }
        return filtered;
    }

    // =========================================================================
    // 7. ListConsumerConnection
    // =========================================================================

    @Override
    public void listConsumerConnection(ListConsumerConnectionRequest request,
        StreamObserver<ListConsumerConnectionResponse> responseObserver) {
        String group = request.getGroup().getName();
        if (group.isEmpty()) {
            respond(responseObserver, ListConsumerConnectionResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "group is required")).build());
            return;
        }
        String topicFilter = request.hasTopic() && !request.getTopic().getName().isEmpty()
            ? request.getTopic().getName() : null;
        allMasterBrokerAddrs()
            .thenCompose(brokers -> collectConnections(brokers, Collections.singletonList(group)))
            .whenComplete((byGroup, throwable) -> {
                if (throwable != null) {
                    // an offline group is a normal answer for a listing RPC, not a server error
                    log.info("listConsumerConnection group={} unavailable: {}", group, throwable.getMessage());
                    respond(responseObserver, ListConsumerConnectionResponse.newBuilder()
                        .setStatus(ok()).build());
                    return;
                }
                try {
                    ConsumerConnection merged = byGroup.get(group);
                    ListConsumerConnectionResponse.Builder builder =
                        ListConsumerConnectionResponse.newBuilder().setStatus(ok());
                    if (merged != null) {
                        if (topicFilter != null && (merged.getSubscriptionTable() == null
                            || !merged.getSubscriptionTable().containsKey(topicFilter))) {
                            respond(responseObserver, builder.build());
                            return;
                        }
                        apache.rocketmq.v2.MessageModel messageModel =
                            AdminModelConverter.toMessageModel(merged.getMessageModel());
                        for (Connection connection : merged.getConnectionSet()) {
                            builder.addClientInfo(AdminModelConverter.toClientInfo(connection, messageModel));
                        }
                    }
                    respond(responseObserver, builder.build());
                } catch (Throwable t) {
                    respond(responseObserver, ListConsumerConnectionResponse.newBuilder()
                        .setStatus(err(t)).build());
                }
            });
    }

    // =========================================================================
    // 8. ResetGroupOffset
    // =========================================================================

    @Override
    public void resetGroupOffset(ResetGroupOffsetRequest request, StreamObserver<ResetGroupOffsetResponse> responseObserver) {
        String group = request.getGroup().getName();
        String topic = request.getTopic().getName();
        if (group.isEmpty() || topic.isEmpty()) {
            respond(responseObserver, ResetGroupOffsetResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "group and topic are required")).build());
            return;
        }
        // An unset protobuf Timestamp is 0, which would silently mean "replay everything since 1970".
        if (!request.hasResetTimestamp() || request.getResetTimestamp().getSeconds() <= 0) {
            respond(responseObserver, ResetGroupOffsetResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "reset_timestamp is required and must be a positive Unix time"))
                .build());
            return;
        }
        long resetTimestamp = TimeUnit.SECONDS.toMillis(request.getResetTimestamp().getSeconds())
            + TimeUnit.NANOSECONDS.toMillis(request.getResetTimestamp().getNanos());
        List<String> brokerAddrs;
        try {
            brokerAddrs = topicBrokerAddrs(topic, true);
        } catch (Throwable t) {
            respond(responseObserver, ResetGroupOffsetResponse.newBuilder().setStatus(err(t)).build());
            return;
        }
        log.info("resetGroupOffset group={} topic={} timestamp={} brokers={}", group, topic, resetTimestamp, brokerAddrs);
        queryAllBrokers(brokerAddrs, (addr, timeout) ->
                admin().resetOffset(addr, topic, group, resetTimestamp, true, timeout))
            .whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, ResetGroupOffsetResponse.newBuilder().setStatus(err(throwable)).build());
                } else {
                    respond(responseObserver, ResetGroupOffsetResponse.newBuilder().setStatus(ok()).build());
                }
            });
    }

    // =========================================================================
    // 9. QueryMessage
    // =========================================================================

    @Override
    public void queryMessage(ListMessageRequest request, StreamObserver<ListMessageResponse> responseObserver) {
        String topic = request.getTopic().getName();
        if (topic.isEmpty()) {
            respond(responseObserver, ListMessageResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "topic is required")).build());
            return;
        }
        int maxNums = request.getMaxMessageNums() > 0 ? request.getMaxMessageNums() : DEFAULT_MAX_MESSAGE_NUMS;
        long begin = request.hasBeginTimestamp() ? TimeUnit.SECONDS.toMillis(request.getBeginTimestamp().getSeconds()) : 0L;
        long end = request.hasEndTimestamp() ? TimeUnit.SECONDS.toMillis(request.getEndTimestamp().getSeconds())
            : Long.MAX_VALUE;

        List<String> brokerAddrs;
        try {
            brokerAddrs = topicBrokerAddrs(topic, false);
        } catch (Throwable t) {
            respond(responseObserver, ListMessageResponse.newBuilder().setStatus(err(t)).build());
            return;
        }

        String key;
        boolean uniqueKey;
        switch (request.getSearchKeyCase()) {
            case MESSAGE_ID:
                // a v2 message_id is the client-generated unique key, not the broker's offset-encoded
                // id, so it must be looked up through the unique-key index on every broker that
                // hosts the topic — decoding it as an offset would query unrelated data
                key = request.getMessageId();
                uniqueKey = true;
                break;
            case MESSAGE_KEY:
                key = request.getMessageKey();
                uniqueKey = false;
                break;
            case SUBSCRIPTION:
            case LITE_TOPIC:
            case SEARCHKEY_NOT_SET:
            default:
                respond(responseObserver, ListMessageResponse.newBuilder()
                    .setStatus(err(Code.BAD_REQUEST,
                        "querying by " + request.getSearchKeyCase() + " is not supported; "
                            + "message_id or message_key is required"))
                    .build());
                return;
        }
        if (key == null || key.isEmpty()) {
            respond(responseObserver, ListMessageResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "the search key must not be empty")).build());
            return;
        }

        final String queryKey = key;
        final boolean queryUniqueKey = uniqueKey;
        queryAllBrokers(brokerAddrs, (addr, timeout) ->
                admin().queryMessage(addr, topic, queryKey, maxNums, begin, end, queryUniqueKey, true, timeout))
            .whenComplete((byBroker, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, ListMessageResponse.newBuilder().setStatus(err(throwable)).build());
                    return;
                }
                ListMessageResponse.Builder builder = ListMessageResponse.newBuilder().setStatus(ok());
                if (request.hasScrollId()) {
                    builder.setScrollId(request.getScrollId());
                }
                int count = 0;
                outer:
                for (List<MessageExt> messages : byBroker.values()) {
                    if (messages == null) {
                        continue;
                    }
                    for (MessageExt messageExt : messages) {
                        if (messageExt == null) {
                            continue;
                        }
                        if (count >= maxNums) {
                            break outer;
                        }
                        // reuse the data plane converter so born/store timestamps, hosts, delivery
                        // time, message group and user properties are all carried over
                        builder.addMessages(GrpcConverter.getInstance().buildMessage(messageExt));
                        count++;
                    }
                }
                if (count == 0) {
                    builder.setStatus(err(Code.MESSAGE_NOT_FOUND,
                        "no message found for " + (queryUniqueKey ? "message_id " : "message_key ") + queryKey));
                }
                respond(responseObserver, builder.build());
            });
    }

    // =========================================================================
    // 10. PrintThreadStackTrace
    // =========================================================================

    @Override
    public void printThreadStackTrace(PrintThreadStackTraceRequest request,
        StreamObserver<PrintThreadStackTraceResponse> responseObserver) {
        String group = request.getGroup().getName();
        String clientId = request.getClientId();
        if (forwarder.forwardIfRemote(group, clientId, responseObserver,
            (stub, observer) -> stub.printThreadStackTrace(request, observer))) {
            return;
        }
        relayConsumerRunningInfo(group, clientId, true)
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, PrintThreadStackTraceResponse.newBuilder()
                        .setStatus(err(throwable)).build());
                    return;
                }
                org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo runningInfo = result.getResult();
                String jstack = runningInfo == null ? null : runningInfo.getJstack();
                if (result.getCode() != ResponseCode.SUCCESS || jstack == null || jstack.isEmpty()) {
                    respond(responseObserver, PrintThreadStackTraceResponse.newBuilder()
                        .setStatus(err(Code.NOT_FOUND,
                            "client " + clientId + " did not return a thread stack: " + result.getRemark()))
                        .build());
                    return;
                }
                respond(responseObserver, PrintThreadStackTraceResponse.newBuilder()
                    .setStatus(ok())
                    .setThreadStackTrace(jstack)
                    .build());
            });
    }

    // =========================================================================
    // 11. VerifyMessage
    // =========================================================================

    @Override
    public void verifyMessage(VerifyMessageRequest request, StreamObserver<VerifyMessageResponse> responseObserver) {
        String group = request.getGroup().getName();
        String clientId = request.getClientId();
        String topic = request.getTopic().getName();
        String messageId = request.getMessageId();
        if (group.isEmpty() || clientId.isEmpty() || topic.isEmpty() || messageId.isEmpty()) {
            respond(responseObserver, VerifyMessageResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "group, client_id, topic and message_id are required")).build());
            return;
        }
        if (forwarder.forwardIfRemote(group, clientId, responseObserver,
            (stub, observer) -> stub.verifyMessage(request, observer))) {
            return;
        }
        // the real message has to be fetched first: asking a client to consume an empty shell would
        // prove nothing about its consumer logic
        List<String> brokerAddrs;
        try {
            brokerAddrs = topicBrokerAddrs(topic, false);
        } catch (Throwable t) {
            respond(responseObserver, VerifyMessageResponse.newBuilder().setStatus(err(t)).build());
            return;
        }
        // The message is fetched with its body still compressed: it is handed to the client
        // verbatim, so inflating it here would leave the body and the compression flag the client
        // reads disagreeing with each other.
        queryAllBrokers(brokerAddrs, (addr, timeout) ->
                admin().queryMessage(addr, topic, messageId, 1, 0L, Long.MAX_VALUE, true, false, timeout))
            .whenComplete((byBroker, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, VerifyMessageResponse.newBuilder().setStatus(err(throwable)).build());
                    return;
                }
                MessageExt messageExt = null;
                for (List<MessageExt> messages : byBroker.values()) {
                    if (messages != null && !messages.isEmpty() && messages.get(0) != null) {
                        messageExt = messages.get(0);
                        break;
                    }
                }
                if (messageExt == null) {
                    // "no such message" is a normal answer, not a proxy fault
                    respond(responseObserver, VerifyMessageResponse.newBuilder()
                        .setStatus(err(Code.MESSAGE_NOT_FOUND,
                            "message " + messageId + " not found on topic " + topic))
                        .build());
                    return;
                }
                relayConsumeMessageDirectly(group, clientId, messageExt)
                    .whenComplete((result, relayThrowable) -> {
                        if (relayThrowable != null) {
                            respond(responseObserver, VerifyMessageResponse.newBuilder()
                                .setStatus(err(relayThrowable)).build());
                            return;
                        }
                        ConsumeMessageDirectlyResult directlyResult = result.getResult();
                        CMResult consumeResult = directlyResult == null ? null : directlyResult.getConsumeResult();
                        if (consumeResult == CMResult.CR_SUCCESS) {
                            respond(responseObserver, VerifyMessageResponse.newBuilder().setStatus(ok()).build());
                        } else {
                            respond(responseObserver, VerifyMessageResponse.newBuilder()
                                .setStatus(err(Code.MESSAGE_CORRUPTED,
                                    "client " + clientId + " failed to consume message " + messageId + ": "
                                        + (consumeResult == null ? result.getRemark() : consumeResult.name())
                                        + (directlyResult != null && directlyResult.getRemark() != null
                                            ? " (" + directlyResult.getRemark() + ")" : "")))
                                .build());
                        }
                    });
            });
    }

    // =========================================================================
    // 12. AdminSendMessage
    // =========================================================================

    @Override
    public void adminSendMessage(AdminSendMessageRequest request, StreamObserver<AdminSendMessageResponse> responseObserver) {
        String topic = request.getTopic().getName();
        if (topic.isEmpty()) {
            respond(responseObserver, AdminSendMessageResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "topic is required")).build());
            return;
        }
        org.apache.rocketmq.common.message.Message message;
        try {
            message = buildAdminMessage(request);
        } catch (Throwable t) {
            // building the message validates caller-supplied properties, so a rejection has to be
            // answered with a Status; letting it escape this method would reply with a bare gRPC
            // error carrying no Status at all
            respond(responseObserver, AdminSendMessageResponse.newBuilder().setStatus(err(t)).build());
            return;
        }
        // ext_info is request-scoped and unknown keys must be ignored, so it is only logged here
        if (!request.getExtInfoMap().isEmpty()) {
            log.info("adminSendMessage ext_info={}", request.getExtInfoMap());
        }

        List<org.apache.rocketmq.common.message.Message> messages = new ArrayList<>();
        messages.add(message);
        final String shardingKey = message.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
        long timeout = timeoutMillis();
        CompletableFuture<List<SendResult>> sendFuture;
        try {
            sendFuture = messagingProcessor.sendMessage(ctx(),
                (queueContext, messageQueueView) -> selectQueue(messageQueueView, shardingKey),
                ADMIN_SEND_PRODUCER_GROUP, 0, messages, timeout);
        } catch (Throwable t) {
            respond(responseObserver, AdminSendMessageResponse.newBuilder().setStatus(err(t)).build());
            return;
        }
        withTimeout(sendFuture, timeout * 2, "adminSendMessage")
            .whenComplete((sendResults, throwable) -> {
                if (throwable != null) {
                    respond(responseObserver, AdminSendMessageResponse.newBuilder().setStatus(err(throwable)).build());
                    return;
                }
                String messageId = sendResults != null && !sendResults.isEmpty() && sendResults.get(0) != null
                    ? sendResults.get(0).getMsgId() : "";
                respond(responseObserver, AdminSendMessageResponse.newBuilder()
                    .setStatus(ok())
                    .setMessageId(messageId == null ? "" : messageId)
                    .build());
            });
    }

    /**
     * Builds the message an admin send request describes, mirroring what the data plane's
     * {@code SendMessageActivity} does for a producer.
     *
     * <p>System properties are written with {@link MessageAccessor#putProperty}, never with
     * {@code Message#putUserProperty}: the latter rejects every name in
     * {@link MessageConst#STRING_HASH_SET}, so using it for a system property such as the delivery
     * timestamp throws instead of sending the message. Caller-supplied user properties are checked
     * against that same set first and refused as {@link Code#ILLEGAL_MESSAGE_PROPERTY_KEY}, so a
     * request that tries to forge a system property gets a graded answer rather than an internal
     * error.
     */
    private static org.apache.rocketmq.common.message.Message buildAdminMessage(AdminSendMessageRequest request) {
        org.apache.rocketmq.common.message.Message message = new org.apache.rocketmq.common.message.Message(
            request.getTopic().getName(), request.getBody().toByteArray());
        if (request.hasTag() && !request.getTag().isEmpty()) {
            message.setTags(request.getTag());
        }
        if (request.hasKey() && !request.getKey().isEmpty()) {
            message.setKeys(request.getKey());
        }
        for (Map.Entry<String, String> property : request.getUserPropertiesMap().entrySet()) {
            if (MessageConst.STRING_HASH_SET.contains(property.getKey())) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY,
                    "property is used by system: " + property.getKey());
            }
            MessageAccessor.putProperty(message, property.getKey(), property.getValue());
        }
        if (!request.hasSystemProperties()) {
            return message;
        }
        SystemProperties systemProperties = request.getSystemProperties();
        if (systemProperties.hasDeliveryTimestamp()) {
            Timestamp deliveryTimestamp = systemProperties.getDeliveryTimestamp();
            if (!Timestamps.isValid(deliveryTimestamp)) {
                throw new GrpcProxyException(Code.ILLEGAL_DELIVERY_TIME,
                    "delivery_timestamp is not a valid timestamp");
            }
            long deliverAt = Timestamps.toMillis(deliveryTimestamp);
            if (deliverAt <= System.currentTimeMillis()) {
                throw new GrpcProxyException(Code.ILLEGAL_DELIVERY_TIME,
                    "delivery_timestamp must be in the future");
            }
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(deliverAt));
        }
        if (!systemProperties.getMessageGroup().isEmpty()) {
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_SHARDING_KEY,
                systemProperties.getMessageGroup());
        }
        for (String key : systemProperties.getKeysList()) {
            if (!key.isEmpty()) {
                message.setKeys(key);
                break;
            }
        }
        if (systemProperties.hasTag() && !systemProperties.getTag().isEmpty()) {
            message.setTags(systemProperties.getTag());
        }
        return message;
    }

    /**
     * A FIFO message must keep going to the queue its message group hashes to; anything else is
     * spread over the writable queues instead of always landing on queue 0.
     */
    private static AddressableMessageQueue selectQueue(MessageQueueView messageQueueView, String shardingKey) {
        List<AddressableMessageQueue> queues = messageQueueView.getWriteSelector().getQueues();
        if (queues == null || queues.isEmpty()) {
            throw new IllegalStateException("no writable queue available");
        }
        if (shardingKey != null && !shardingKey.isEmpty()) {
            int index = Math.floorMod(shardingKey.hashCode(), queues.size());
            return queues.get(index);
        }
        return queues.get(ThreadLocalRandom.current().nextInt(queues.size()));
    }

    // =========================================================================
    // 13. GetConsumerRunningInfo
    // =========================================================================

    @Override
    public void getConsumerRunningInfo(GetConsumerRunningInfoRequest request,
        StreamObserver<GetConsumerRunningInfoResponse> responseObserver) {
        String group = request.getGroup().getName();
        String clientId = request.getClientId();
        if (clientId.isEmpty()) {
            respond(responseObserver, GetConsumerRunningInfoResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "client_id is required")).build());
            return;
        }
        if (forwarder.forwardIfRemote(group, clientId, responseObserver,
            (stub, observer) -> stub.getConsumerRunningInfo(request, observer))) {
            return;
        }
        relayConsumerRunningInfo(group, clientId, false)
            .whenComplete((result, throwable) -> {
                if (throwable == null && result.getCode() == ResponseCode.SUCCESS && result.getResult() != null) {
                    respond(responseObserver, GetConsumerRunningInfoResponse.newBuilder()
                        .setStatus(ok())
                        .setConsumerRunningInfo(AdminModelConverter.toConsumerRunningInfo(result.getResult()))
                        .build());
                    return;
                }
                // A gRPC v2 client cannot report running info: the telemetry contract has no reply
                // message carrying properties, process queue snapshots or consume statistics. Only
                // remoting clients can. Rather than returning an empty shell that looks like a
                // healthy consumer, fall back to what the proxy does know and say so.
                ConsumerRunningInfo.Builder builder = ConsumerRunningInfo.newBuilder();
                apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
                if (settings == null || !settings.hasSubscription()) {
                    respond(responseObserver, GetConsumerRunningInfoResponse.newBuilder()
                        .setStatus(err(throwable != null ? throwable
                            : new IllegalStateException("client " + clientId + " is not connected to this cluster")))
                        .build());
                    return;
                }
                for (apache.rocketmq.v2.SubscriptionEntry entry : settings.getSubscription().getSubscriptionsList()) {
                    builder.putSubscriptions(entry.hasTopic() ? entry.getTopic().getName() : "",
                        entry.hasExpression() ? entry.getExpression()
                            : apache.rocketmq.v2.FilterExpression.newBuilder()
                                .setType(apache.rocketmq.v2.FilterType.TAG).setExpression("*").build());
                }
                // The client was found and its subscriptions are returned, so this is a success with
                // an explanatory message rather than NOT_FOUND, which would claim the client does not
                // exist and hide the fact that usable data came back.
                respond(responseObserver, GetConsumerRunningInfoResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK,
                        "only subscriptions are available: a gRPC v2 client does not report properties, "
                            + "process queue snapshots or consume statistics over the telemetry channel; "
                            + "use a remoting client for the full running info"))
                    .setConsumerRunningInfo(builder.build())
                    .build());
            });
    }

    // =========================================================================
    // 14. GetTopicRoute
    // =========================================================================

    @Override
    public void getTopicRoute(GetTopicRouteRequest request, StreamObserver<GetTopicRouteResponse> responseObserver) {
        String topic = request.getTopic().getName();
        if (topic.isEmpty()) {
            respond(responseObserver, GetTopicRouteResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "topic is required")).build());
            return;
        }
        admin().getTopicRouteData(topic).whenComplete((topicRouteData, throwable) -> {
            if (throwable != null) {
                respond(responseObserver, GetTopicRouteResponse.newBuilder().setStatus(err(throwable)).build());
                return;
            }
            // topic_route_data is documented as the serialized route table, so the request's
            // network_type / protocol_type / client_address hints have no bearing on it: they only
            // matter when the response carries protocol-specific access points, which this one does not
            respond(responseObserver, AdminModelConverter.toTopicRoute((TopicRouteData) topicRouteData));
        });
    }

    // =========================================================================
    // 15. QueryTimeSpan
    // =========================================================================

    @Override
    public void queryTimeSpan(QueryTimeSpanRequest request, StreamObserver<QueryTimeSpanResponse> responseObserver) {
        String group = request.getGroup().getName();
        if (group.isEmpty()) {
            respond(responseObserver, QueryTimeSpanResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "group is required")).build());
            return;
        }
        Set<String> topics = new LinkedHashSet<>();
        for (Resource resource : request.getTopicsList()) {
            if (!resource.getName().isEmpty()) {
                topics.add(resource.getName());
            }
        }
        if (topics.isEmpty()) {
            respond(responseObserver, QueryTimeSpanResponse.newBuilder()
                .setStatus(err(Code.BAD_REQUEST, "at least one topic is required")).build());
            return;
        }
        // every requested topic is queried, on every broker hosting it; the broker computes the
        // per-queue min/max/consume timestamps and delay itself (QUERY_CONSUME_TIME_SPAN)
        List<CompletableFuture<List<QueueTimeSpan>>> tasks = new ArrayList<>();
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        for (String topic : topics) {
            List<String> brokerAddrs;
            try {
                brokerAddrs = topicBrokerAddrs(topic, false);
            } catch (Throwable t) {
                failures.add(t);
                log.warn("queryTimeSpan cannot resolve brokers for topic {} group {}", topic, group, t);
                continue;
            }
            tasks.add(queryAllBrokers(brokerAddrs, (addr, timeout) ->
                    admin().queryConsumeTimeSpan(addr, topic, group, timeout))
                .thenApply(byBroker -> {
                    List<QueueTimeSpan> spans = new ArrayList<>();
                    for (List<QueueTimeSpan> perBroker : byBroker.values()) {
                        if (perBroker != null) {
                            spans.addAll(perBroker);
                        }
                    }
                    return spans;
                })
                .exceptionally(t -> {
                    failures.add(t);
                    return Collections.emptyList();
                }));
        }
        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).whenComplete((ignored, throwable) -> {
            List<QueueTimeSpan> all = new ArrayList<>();
            for (CompletableFuture<List<QueueTimeSpan>> task : tasks) {
                List<QueueTimeSpan> spans = task.getNow(null);
                if (spans != null) {
                    all.addAll(spans);
                }
            }
            if (all.isEmpty() && !failures.isEmpty()) {
                respond(responseObserver, QueryTimeSpanResponse.newBuilder().setStatus(err(failures.get(0))).build());
                return;
            }
            try {
                respond(responseObserver, AdminModelConverter.toQueryTimeSpan(all));
            } catch (Throwable t) {
                respond(responseObserver, QueryTimeSpanResponse.newBuilder().setStatus(err(t)).build());
            }
        });
    }

    // =========================================================================
    // 16. GetProxyRuntimeStats
    // =========================================================================

    @Override
    public void getProxyRuntimeStats(GetProxyRuntimeStatsRequest request,
        StreamObserver<GetProxyRuntimeStatsResponse> responseObserver) {
        try {
            long producers = 0L;
            long consumers = 0L;
            Collection<GrpcClientChannel> channels = grpcChannelManager.getClientChannels();
            for (GrpcClientChannel channel : channels) {
                apache.rocketmq.v2.Settings settings = grpcClientSettingsManager.getRawClientSettings(channel.getClientId());
                if (settings == null) {
                    continue;
                }
                switch (settings.getClientType()) {
                    case PRODUCER:
                        producers++;
                        break;
                    case PUSH_CONSUMER:
                    case SIMPLE_CONSUMER:
                    case PULL_CONSUMER:
                    case LITE_PUSH_CONSUMER:
                    case LITE_SIMPLE_CONSUMER:
                        consumers++;
                        break;
                    default:
                        break;
                }
            }
            // in_tps / out_tps stay unset on purpose: the open-source proxy keeps no per-process
            // throughput counter, and reporting 0 would be indistinguishable from an idle proxy
            respond(responseObserver, GetProxyRuntimeStatsResponse.newBuilder()
                .setStatus(ok())
                .setProxyName(ConfigurationManager.getProxyConfig().getProxyName())
                .setVersion(MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION))
                .setConnections(channels.size())
                .setProducers(producers)
                .setConsumers(consumers)
                .build());
        } catch (Throwable t) {
            log.warn("getProxyRuntimeStats failed", t);
            respond(responseObserver, GetProxyRuntimeStatsResponse.newBuilder().setStatus(err(t)).build());
        }
    }

    // =========================================================================
    // client relay
    // =========================================================================

    /**
     * Asks a connected client for its running info through the proxy's in-process relay. The result
     * future is carried on the {@link ProxyRelayRequest} so that
     * {@code ProxyChannel.writeAndFlush} can hand it to the channel implementation, which completes
     * it when the client answers.
     */
    private CompletableFuture<ProxyRelayResult<org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo>>
        relayConsumerRunningInfo(String group, String clientId, boolean jstack) {
        CompletableFuture<ProxyRelayResult<org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo>> future =
            new CompletableFuture<>();
        ClientChannelInfo channelInfo = findClientChannel(group, clientId);
        if (channelInfo == null || channelInfo.getChannel() == null) {
            future.complete(new ProxyRelayResult<>(ResponseCode.SYSTEM_ERROR,
                "client " + clientId + " is not connected to this cluster", null));
            return future;
        }
        if (!channelInfo.getChannel().isActive()) {
            future.complete(new ProxyRelayResult<>(ResponseCode.SYSTEM_ERROR,
                "client " + clientId + " channel is inactive", null));
            return future;
        }
        try {
            GetConsumerRunningInfoRequestHeader header = new GetConsumerRunningInfoRequestHeader();
            header.setConsumerGroup(group);
            header.setClientId(clientId);
            header.setJstackEnable(jstack);
            ProxyRelayRequest relayRequest = ProxyRelayRequest.createRequestCommand(
                RequestCode.GET_CONSUMER_RUNNING_INFO, header, future);
            channelInfo.getChannel().writeAndFlush(relayRequest);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }
        return withTimeout(future, relayTimeoutMillis(), "getConsumerRunningInfo for client " + clientId);
    }

    private CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> relayConsumeMessageDirectly(
        String group, String clientId, MessageExt messageExt) {
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> future = new CompletableFuture<>();
        ClientChannelInfo channelInfo = findClientChannel(group, clientId);
        if (channelInfo == null || channelInfo.getChannel() == null) {
            future.complete(new ProxyRelayResult<>(ResponseCode.SYSTEM_ERROR,
                "client " + clientId + " is not connected to this cluster", null));
            return future;
        }
        try {
            ConsumeMessageDirectlyResultRequestHeader header = new ConsumeMessageDirectlyResultRequestHeader();
            header.setConsumerGroup(group);
            header.setClientId(clientId);
            header.setTopic(messageExt.getTopic());
            header.setMsgId(messageExt.getMsgId());
            // the header requires a broker name although the write never reaches a broker; the
            // message's own store queue is the most meaningful value available here
            header.setBrokerName(messageExt.getBrokerName() == null ? "" : messageExt.getBrokerName());
            ProxyRelayRequest relayRequest = ProxyRelayRequest.createRequestCommand(
                RequestCode.CONSUME_MESSAGE_DIRECTLY, header, future);
            // storeSize is what the broker recorded on disk and MessageDecoder.encode allocates
            // exactly that many bytes; zero it so the size is recomputed from what is actually being
            // written, or a body that no longer matches the stored one overflows the buffer
            messageExt.setStoreSize(0);
            relayRequest.setBody(org.apache.rocketmq.common.message.MessageDecoder.encode(messageExt, false));
            channelInfo.getChannel().writeAndFlush(relayRequest);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }
        return withTimeout(future, relayTimeoutMillis(), "verifyMessage for client " + clientId);
    }

    private long relayTimeoutMillis() {
        return TimeUnit.SECONDS.toMillis(
            ConfigurationManager.getProxyConfig().getGrpcProxyRelayRequestTimeoutInSeconds()) + 1000L;
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
        this.adminTimeoutScheduler.shutdownNow();
    }

    @Override
    public String toString() {
        return "ProxyAdminGrpcService{proxyName=" + ConfigurationManager.getProxyConfig().getProxyName()
            + ", adminPort=" + ConfigurationManager.getProxyConfig().getGrpcAdminServerPort() + "}";
    }
}
