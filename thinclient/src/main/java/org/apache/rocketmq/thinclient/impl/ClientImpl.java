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

package org.apache.rocketmq.thinclient.impl;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageCommand;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.Metadata;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.thinclient.exception.ResourceNotFoundException;
import org.apache.rocketmq.thinclient.hook.MessageHookPoints;
import org.apache.rocketmq.thinclient.hook.MessageHookPointsStatus;
import org.apache.rocketmq.thinclient.hook.MessageInterceptor;
import org.apache.rocketmq.thinclient.message.MessageCommon;
import org.apache.rocketmq.thinclient.metrics.MessageMeter;
import org.apache.rocketmq.thinclient.metrics.Metric;
import org.apache.rocketmq.thinclient.rpc.Signature;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.route.TopicRouteData;
import org.apache.rocketmq.thinclient.route.TopicRouteDataResult;
import org.apache.rocketmq.thinclient.misc.ExecutorServices;
import org.apache.rocketmq.thinclient.misc.ThreadFactoryImpl;
import org.apache.rocketmq.thinclient.misc.Utilities;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public abstract class ClientImpl extends AbstractIdleService implements Client, MessageInterceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientImpl.class);
    private static final Duration TOPIC_ROUTE_AWAIT_DURATION_DURING_STARTUP = Duration.ofSeconds(3);

    protected volatile ClientManager clientManager;
    protected final ClientConfiguration clientConfiguration;
    protected final Endpoints accessEndpoints;

    private volatile ScheduledFuture<?> updateRouteCacheFuture;

    protected final Set<String> topics;
    private final ConcurrentMap<String, TopicRouteDataResult> topicRouteResultCache;

    @GuardedBy("inflightRouteFutureLock")
    private final Map<String /* topic */, Set<SettableFuture<TopicRouteDataResult>>> inflightRouteFutureTable;
    private final Lock inflightRouteFutureLock;

    @GuardedBy("telemetrySessionsLock")
    private final ConcurrentMap<Endpoints, TelemetrySession> telemetrySessionTable;
    private final ReadWriteLock telemetrySessionsLock;

    // Thread-safe set.
    protected final Set<Endpoints> isolated;

    @GuardedBy("messageInterceptorsLock")
    private final List<MessageInterceptor> messageInterceptors;
    private final ReadWriteLock messageInterceptorsLock;

    protected final ExecutorService clientCallbackExecutor;

    protected final MessageMeter messageMeter;

    /**
     * Telemetry command executor, which is aims to execute commands from remote.
     */
    protected final ThreadPoolExecutor telemetryCommandExecutor;

    protected final String clientId;

    public ClientImpl(ClientConfiguration clientConfiguration, Set<String> topics) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        final String accessPoint = clientConfiguration.getAccessPoint();
        this.accessEndpoints = new Endpoints(accessPoint);
        this.topics = topics;
        // Generate client id firstly.
        this.clientId = Utilities.genClientId();

        this.topicRouteResultCache = new ConcurrentHashMap<>();

        this.inflightRouteFutureTable = new ConcurrentHashMap<>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.telemetrySessionTable = new ConcurrentHashMap<>();
        this.telemetrySessionsLock = new ReentrantReadWriteLock();

        this.isolated = Collections.newSetFromMap(new ConcurrentHashMap<>());

        this.messageInterceptors = new ArrayList<>();
        this.messageInterceptorsLock = new ReentrantReadWriteLock();

        this.clientCallbackExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("ClientCallbackWorker"));

        this.messageMeter = new MessageMeter(this);

        this.telemetryCommandExecutor = new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("CommandExecutor"));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("JVM shutdown hook is invoked, clientId={}, state={}", clientId, ClientImpl.this.state());
            ClientImpl.this.stopAsync().awaitTerminated();
        }));
    }

    /**
     * Start the rocketmq client and do some preparatory work.
     */
    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Begin to start the rocketmq client, clientId={}", clientId);
        // Register client after client id generation.
        this.clientManager = ClientManagerRegistry.getInstance().registerClient(this);
        // Fetch topic route from remote.
        LOGGER.info("Begin to fetch topic(s) route data from remote during client startup, clientId={}, topics={}", clientId, topics);
        // Aggregate all topic route data futures into a composited future.
        final List<ListenableFuture<TopicRouteDataResult>> futures = topics.stream()
            .map(this::getRouteDataResult)
            .collect(Collectors.toList());
        List<TopicRouteDataResult> results;
        try {
            results = Futures.allAsList(futures).get(TOPIC_ROUTE_AWAIT_DURATION_DURING_STARTUP.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            LOGGER.error("Failed to get topic route data result from remote during client startup, clientId={}, topics={}", clientId, topics, t);
            throw new ResourceNotFoundException(t);
        }
        // Find any topic whose topic route data is failed to fetch from remote.
        final Stream<TopicRouteDataResult> stream = results.stream()
            .filter(topicRouteDataResult -> Code.OK != topicRouteDataResult.getStatus().getCode());
        final Optional<TopicRouteDataResult> any = stream.findAny();
        // There is a topic whose topic route data is failed to fetch from remote.
        if (any.isPresent()) {
            final TopicRouteDataResult result = any.get();
            final Status status = result.getStatus();
            throw new ClientException(status.getCode().getNumber(), status.getMessage());
        }
        LOGGER.info("Fetch topic route data from remote successfully during startup, clientId={}, topics={}", clientId, topics);
        // Update route cache periodically.
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        this.updateRouteCacheFuture = scheduler.scheduleWithFixedDelay(() -> {
            try {
                updateRouteCache();
            } catch (Throwable t) {
                LOGGER.error("Exception raised while updating topic route cache, clientId={}", clientId, t);
            }
        }, 10, 30, TimeUnit.SECONDS);
        LOGGER.info("The rocketmq client starts successfully, clientId={}", clientId);
    }

    /**
     * Shutdown the rocketmq client and release related resources.
     */
    @Override
    protected void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq client, clientId={}", clientId);
        notifyClientTermination();
        if (null != this.updateRouteCacheFuture) {
            updateRouteCacheFuture.cancel(false);
        }
        telemetryCommandExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(telemetryCommandExecutor)) {
            LOGGER.error("[Bug] Timeout to shutdown the telemetry command executor, clientId={}", clientId);
        } else {
            LOGGER.info("Shutdown the telemetry command executor successfully, clientId={}", clientId);
        }
        LOGGER.info("Begin to release telemetry sessions, clientId={}", clientId);
        releaseTelemetrySessions();
        LOGGER.info("Release telemetry sessions successfully, clientId={}", clientId);
        ClientManagerRegistry.getInstance().unregisterClient(this);
        clientCallbackExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(clientCallbackExecutor)) {
            LOGGER.error("[Bug] Timeout to shutdown the client callback executor, clientId={}", clientId);
        }
        LOGGER.info("Shutdown the rocketmq client successfully, clientId={}", clientId);
    }

    public void registerMessageInterceptor(MessageInterceptor messageInterceptor) {
        messageInterceptorsLock.writeLock().lock();
        try {
            messageInterceptors.add(messageInterceptor);
        } finally {
            messageInterceptorsLock.writeLock().unlock();
        }
    }

    @Override
    public void doBefore(MessageHookPoints hookPoint, List<MessageCommon> messageCommons) {
        messageInterceptorsLock.readLock().lock();
        try {
            for (MessageInterceptor interceptor : messageInterceptors) {
                try {
                    interceptor.doBefore(hookPoint, messageCommons);
                } catch (Throwable t) {
                    LOGGER.warn("Exception raised while intercepting message, hookPoint={}, clientId={}", hookPoint, clientId);
                }
            }
        } finally {
            messageInterceptorsLock.readLock().unlock();
        }
    }

    @Override
    public void doAfter(MessageHookPoints hookPoints, List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status) {
        messageInterceptorsLock.readLock().lock();
        try {
            for (MessageInterceptor interceptor : messageInterceptors) {
                try {
                    interceptor.doAfter(hookPoints, messageCommons, duration, status);
                } catch (Throwable t) {
                    LOGGER.warn("Exception raised while intercepting message, hookPoint={}, clientId={}", hookPoints, clientId);
                }
            }
        } finally {
            messageInterceptorsLock.readLock().unlock();
        }
    }

    /**
     * This method is invoked while request of printing thread stack trace is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of printing thread stack trace from remote.
     */
    void onPrintThreadStackCommand(Endpoints endpoints, PrintThreadStackTraceCommand command) {
        final String nonce = command.getNonce();
        Runnable task = () -> {
            try {
                final String stackTrace = Utilities.stackTrace();
                Status status = Status.newBuilder().setCode(Code.OK).build();
                ThreadStackTrace threadStackTrace = ThreadStackTrace.newBuilder().setThreadStackTrace(stackTrace)
                    .setNonce(command.getNonce()).build();
                TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder()
                    .setThreadStackTrace(threadStackTrace)
                    .setStatus(status)
                    .build();
                telemeter(endpoints, telemetryCommand);
            } catch (Throwable t) {
                LOGGER.error("Failed to send thread stack trace to remote, endpoints={}, nonce={}, clientId={}", endpoints, nonce, clientId, t);
            }
        };
        try {
            telemetryCommandExecutor.submit(task);
        } catch (Throwable t) {
            LOGGER.error("[Bug] Exception raised while submitting task to print thread stack trace, endpoints={}, nonce={}, clientId={}", endpoints, nonce, clientId, t);
        }
    }

    public abstract ClientSettings getClientSettings();

    /**
     * Apply setting from remote.
     *
     * @param endpoints remote endpoints.
     * @param settings  settings received from remote.
     */
    public final void onSettingsCommand(Endpoints endpoints, Settings settings) {
        final Metric metric = new Metric(settings.getMetric());
        messageMeter.refresh(metric);
        LOGGER.info("Receive settings from remote, endpoints={}", endpoints);
        this.getClientSettings().applySettingsCommand(settings);
    }

    /**
     * @see Client#telemeterSettings()
     */
    @Override
    public void telemeterSettings() {
        final Settings settings = getClientSettings().toProtobuf();
        final TelemetryCommand command = TelemetryCommand.newBuilder().setSettings(settings).build();
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        for (Endpoints endpoints : totalRouteEndpoints) {
            try {
                telemeter(endpoints, command);
            } catch (Throwable t) {
                LOGGER.error("Failed to telemeter settings to remote, clientId={}, endpoints={}", clientId, endpoints, t);
            }
        }
    }

    /**
     * Telemeter command to remote endpoints.
     *
     * @param endpoints remote endpoints to telemeter.
     * @param command   command to telemeter.
     */
    public void telemeter(Endpoints endpoints, TelemetryCommand command) {
        final ListenableFuture<TelemetrySession> future = registerTelemetrySession(endpoints);
        Futures.addCallback(future, new FutureCallback<TelemetrySession>() {
            @Override
            public void onSuccess(TelemetrySession session) {
                try {
                    session.telemeter(command);
                } catch (Throwable t) {
                    LOGGER.error("Failed to telemeter command, endpoints={}, command={}", endpoints, command);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.error("Failed to telemeter command to remote, endpoints={}, command={}", endpoints, command, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private void releaseTelemetrySessions() {
        telemetrySessionsLock.readLock().lock();
        try {
            telemetrySessionTable.values().forEach(TelemetrySession::release);
        } finally {
            telemetrySessionsLock.readLock().unlock();
        }
    }

    /**
     * Try to register telemetry session, return it directly if session is existed already.
     */
    public ListenableFuture<TelemetrySession> registerTelemetrySession(Endpoints endpoints) {
        final SettableFuture<TelemetrySession> future0 = SettableFuture.create();
        telemetrySessionsLock.readLock().lock();
        try {
            TelemetrySession telemetrySession = telemetrySessionTable.get(endpoints);
            // Return is directly if session is existed already.
            if (null != telemetrySession) {
                future0.set(telemetrySession);
                return future0;
            }
        } finally {
            telemetrySessionsLock.readLock().unlock();
        }
        // Future's exception has been logged during the registration.
        final ListenableFuture<TelemetrySession> future = TelemetrySession.register(this, clientManager, endpoints);
        return Futures.transform(future, session -> {
            telemetrySessionsLock.writeLock().lock();
            try {
                TelemetrySession existed = telemetrySessionTable.get(endpoints);
                if (null != existed) {
                    return existed;
                }
                telemetrySessionTable.put(endpoints, session);
                return session;
            } finally {
                telemetrySessionsLock.writeLock().unlock();
            }
        }, MoreExecutors.directExecutor());
    }

    /**
     * Triggered when {@link TopicRouteDataResult} is fetched from remote.
     *
     * <p>Never thrown any exception.
     */
    public ListenableFuture<Void> onTopicRouteDataResultFetched(String topic,
        TopicRouteDataResult topicRouteDataResult) {
        final ListenableFuture<List<TelemetrySession>> future =
            Futures.allAsList(topicRouteDataResult.getTopicRouteData()
                .getMessageQueues().stream()
                .map(mq -> mq.getBroker().getEndpoints())
                .collect(Collectors.toSet())
                .stream().map(this::registerTelemetrySession)
                .collect(Collectors.toList()));
        SettableFuture<Void> future0 = SettableFuture.create();
        Futures.addCallback(future, new FutureCallback<List<TelemetrySession>>() {
            @Override
            public void onSuccess(List<TelemetrySession> sessions) {
                LOGGER.info("Register session successfully, current route will be cached, topic={}, topicRouteDataResult={}", topic, topicRouteDataResult);
                final TopicRouteDataResult old = topicRouteResultCache.put(topic, topicRouteDataResult);
                if (topicRouteDataResult.equals(old)) {
                    // Log if topic route result remains the same.
                    LOGGER.info("Topic route result remains the same, topic={}, clientId={}", topic, clientId);
                } else {
                    // Log if topic route result is updated.
                    LOGGER.info("Topic route result is updated, topic={}, clientId={}, {} => {}", topic, clientId, old, topicRouteDataResult);
                }
                onTopicRouteDataResultUpdate0(topic, topicRouteDataResult);
                future0.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                // Note: Topic route would not be updated if failed to register session.
                LOGGER.error("Failed to register session, current route will NOT be cached, topic={}, topicRouteDataResult={}", topic, topicRouteDataResult);
                future0.setException(t);
            }
        }, MoreExecutors.directExecutor());
        return future0;
    }

    public void onTopicRouteDataResultUpdate0(String topic, TopicRouteDataResult topicRouteDataResult) {
    }

    /**
     * This method is invoked while request of message consume verification is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of message consume verification from remote.
     */
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand command) {
        LOGGER.warn("Ignore verify message command from remote, which is not expected, clientId={}, command={}", clientId, command);
        final String nonce = command.getNonce();
        final Status status = Status.newBuilder().setCode(Code.NOT_IMPLEMENTED).build();
        VerifyMessageResult verifyMessageResult = VerifyMessageResult.newBuilder().setNonce(nonce).build();
        TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder()
            .setVerifyMessageResult(verifyMessageResult)
            .setStatus(status)
            .build();
        try {
            telemeter(endpoints, telemetryCommand);
        } catch (Throwable t) {
            LOGGER.warn("Failed to send message verification result, clientId={}", clientId, t);
        }
    }

    /**
     * This method is invoked while request of orphaned transaction recovery is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of orphaned transaction recovery from remote.
     */
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints, RecoverOrphanedTransactionCommand command) {
        LOGGER.warn("Ignore orphaned transaction recovery command from remote, which is not expected, client id={}, command={}", clientId, command);
    }

    private void updateRouteCache() {
        LOGGER.info("Start to update route cache for a new round, clientId={}", clientId);
        topicRouteResultCache.keySet().forEach(topic -> {
            // Set timeout for future on purpose.
            final ListenableFuture<TopicRouteDataResult> future = Futures.withTimeout(fetchTopicRoute(topic), TOPIC_ROUTE_AWAIT_DURATION_DURING_STARTUP, getScheduler());
            Futures.addCallback(future, new FutureCallback<TopicRouteDataResult>() {
                @Override
                public void onSuccess(TopicRouteDataResult topicRouteDataResult) {
                    onTopicRouteDataResultFetched(topic, topicRouteDataResult);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.error("Failed to fetch topic route for update cache, topic={}, clientId={}", topic, clientId, t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    /**
     * Wrap notify client termination request.
     */
    public abstract NotifyClientTerminationRequest wrapNotifyClientTerminationRequest();

    /**
     * Notify remote that current client is prepared to be terminated.
     */
    private void notifyClientTermination() {
        LOGGER.info("Notify remote that client is terminated, clientId={}", clientId);
        final Set<Endpoints> routeEndpointsSet = getTotalRouteEndpoints();
        final NotifyClientTerminationRequest notifyClientTerminationRequest = wrapNotifyClientTerminationRequest();
        try {
            final Metadata metadata = sign();
            for (Endpoints endpoints : routeEndpointsSet) {
                clientManager.notifyClientTermination(endpoints, metadata, notifyClientTerminationRequest,
                    clientConfiguration.getRequestTimeout());
            }
        } catch (Throwable t) {
            LOGGER.error("Exception raised while notifying client's termination, clientId={}", clientId, t);
        }
    }

    /**
     * @see Client#getClientId()
     */
    @Override
    public String getClientId() {
        return clientId;
    }

    /**
     * @see Client#doHeartbeat()
     */
    @Override
    public void doHeartbeat() {
        final Set<Endpoints> totalEndpoints = getTotalRouteEndpoints();
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : totalEndpoints) {
            doHeartbeat(request, endpoints);
        }
    }

    /**
     * Real-time signature generation
     */
    protected Metadata sign() throws UnsupportedEncodingException, NoSuchAlgorithmException, InvalidKeyException {
        return Signature.sign(clientConfiguration, clientId);
    }

    /**
     * Send heartbeat data to appointed endpoint
     *
     * @param request   heartbeat data request
     * @param endpoints endpoint to send heartbeat data
     */
    private void doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
        try {
            Metadata metadata = sign();
            final ListenableFuture<HeartbeatResponse> future = clientManager
                .heartbeat(endpoints, metadata, request, clientConfiguration.getRequestTimeout());
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getStatus();
                    final Code code = status.getCode();
                    if (Code.OK != code) {
                        LOGGER.warn("Failed to send heartbeat, code={}, status message=[{}], endpoints={}, clientId={}",
                            code, status.getMessage(), endpoints, clientId);
                        return;
                    }
                    LOGGER.info("Send heartbeat successfully, endpoints={}, clientId={}", endpoints, clientId);
                    final boolean removed = isolated.remove(endpoints);
                    if (removed) {
                        LOGGER.info("Rejoin endpoints which is isolated before, clientId={}, endpoints={}", clientId, endpoints);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Failed to send heartbeat, endpoints={}, clientId={}", endpoints, clientId, t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Throwable e) {
            LOGGER.error("Exception raised while preparing heartbeat, endpoints={}, clientId={}", endpoints, clientId, e);
        }
    }

    /**
     * Wrap heartbeat request
     */
    public abstract HeartbeatRequest wrapHeartbeatRequest();

    /**
     * @see Client#doStats()
     */
    @Override
    public void doStats() {
    }

    private ListenableFuture<TopicRouteDataResult> fetchTopicRoute(final String topic) {
        final SettableFuture<TopicRouteDataResult> future = SettableFuture.create();
        try {
            Resource topicResource = Resource.newBuilder().setName(topic).build();
            final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource)
                .setEndpoints(accessEndpoints.toProtobuf()).build();
            final Metadata metadata = sign();
            final ListenableFuture<QueryRouteResponse> responseFuture =
                clientManager.queryRoute(accessEndpoints, metadata, request, clientConfiguration.getRequestTimeout());
            return Futures.transform(responseFuture, response -> {
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.OK != code) {
                    LOGGER.error("Exception raised while fetch topic route from remote, topic={}, " +
                            "clientId={}, endpoints={}, code={}, status message=[{}]", topic, clientId,
                        accessEndpoints, code, status.getMessage());
                }
                return new TopicRouteDataResult(new TopicRouteData(response.getMessageQueuesList()), status);
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            future.setException(t);
            return future;
        }
    }

    protected Set<Endpoints> getTotalRouteEndpoints() {
        Set<Endpoints> totalRouteEndpoints = new HashSet<>();
        for (TopicRouteDataResult result : topicRouteResultCache.values()) {
            totalRouteEndpoints.addAll(result.getTopicRouteData().getTotalEndpoints());
        }
        return totalRouteEndpoints;
    }

    protected ListenableFuture<TopicRouteDataResult> getRouteDataResult(final String topic) {
        SettableFuture<TopicRouteDataResult> future0 = SettableFuture.create();
        TopicRouteDataResult topicRouteDataResult = topicRouteResultCache.get(topic);
        // If route result was cached before, get it directly.
        if (null != topicRouteDataResult) {
            future0.set(topicRouteDataResult);
            return future0;
        }
        inflightRouteFutureLock.lock();
        try {
            // If route was fetched by last in-flight request, get it directly.
            topicRouteDataResult = topicRouteResultCache.get(topic);
            if (null != topicRouteDataResult) {
                future0.set(topicRouteDataResult);
                return future0;
            }
            Set<SettableFuture<TopicRouteDataResult>> inflightFutures = inflightRouteFutureTable.get(topic);
            // Request is in-flight, return future directly.
            if (null != inflightFutures) {
                inflightFutures.add(future0);
                return future0;
            }
            inflightFutures = new HashSet<>();
            inflightFutures.add(future0);
            inflightRouteFutureTable.put(topic, inflightFutures);
        } finally {
            inflightRouteFutureLock.unlock();
        }
        final ListenableFuture<TopicRouteDataResult> future = fetchTopicRoute(topic);
        Futures.addCallback(future, new FutureCallback<TopicRouteDataResult>() {
            @Override
            public void onSuccess(TopicRouteDataResult result) {
                final ListenableFuture<Void> updateFuture = onTopicRouteDataResultFetched(topic, result);
                // TODO: all succeed?
                Futures.whenAllSucceed(updateFuture).run(() -> {
                    inflightRouteFutureLock.lock();
                    try {
                        final Set<SettableFuture<TopicRouteDataResult>> newFutureSet = inflightRouteFutureTable.remove(topic);
                        if (null == newFutureSet) {
                            // Should never reach here.
                            LOGGER.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic, clientId);
                            return;
                        }
                        LOGGER.debug("Fetch topic route successfully, topic={}, in-flight route future "
                            + "size={}, clientId={}", topic, newFutureSet.size(), clientId);
                        for (SettableFuture<TopicRouteDataResult> newFuture : newFutureSet) {
                            newFuture.set(result);
                        }
                    } catch (Throwable t) {
                        // Should never reach here.
                        LOGGER.error("[Bug] Exception raised while update route data, topic={}, clientId={}", topic, clientId, t);
                    } finally {
                        inflightRouteFutureLock.unlock();
                    }
                }, MoreExecutors.directExecutor());
            }

            @Override
            public void onFailure(Throwable t) {
                inflightRouteFutureLock.lock();
                try {
                    final Set<SettableFuture<TopicRouteDataResult>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        LOGGER.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic, clientId);
                        return;
                    }
                    LOGGER.error("Failed to fetch topic route, topic={}, in-flight route future " +
                        "size={}, clientId={}", topic, newFutureSet.size(), clientId, t);
                    for (SettableFuture<TopicRouteDataResult> future : newFutureSet) {
                        future.setException(t);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }
        }, MoreExecutors.directExecutor());
        return future0;
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
    }

    protected <T> T handleClientFuture(ListenableFuture<T> future) throws ClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new ClientException(null == cause ? e : cause);
        }
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }
}
