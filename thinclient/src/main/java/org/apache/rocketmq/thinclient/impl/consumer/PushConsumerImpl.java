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

package org.apache.rocketmq.thinclient.impl.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Metadata;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.rocketmq.apis.ClientConfiguration;
import org.apache.rocketmq.apis.consumer.ConsumeResult;
import org.apache.rocketmq.apis.consumer.FilterExpression;
import org.apache.rocketmq.apis.consumer.MessageListener;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.thinclient.hook.MessageHookPoints;
import org.apache.rocketmq.thinclient.hook.MessageHookPointsStatus;
import org.apache.rocketmq.thinclient.impl.ClientSettings;
import org.apache.rocketmq.thinclient.message.MessageCommon;
import org.apache.rocketmq.thinclient.metrics.MessageCacheObserver;
import org.apache.rocketmq.thinclient.retry.RetryPolicy;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.message.protocol.Resource;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;
import org.apache.rocketmq.thinclient.route.TopicRouteDataResult;
import org.apache.rocketmq.thinclient.misc.ExecutorServices;
import org.apache.rocketmq.thinclient.misc.ThreadFactoryImpl;

/**
 * Default implementation of {@link PushConsumer}
 *
 * <p>It is worth noting that in the implementation of push consumer, the message is not actively pushed by the server
 * to the client, but is obtained by the client actively going to the server.
 *
 * @see PushConsumer
 */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
class PushConsumerImpl extends ConsumerImpl implements PushConsumer, MessageCacheObserver {
    private static final Logger LOGGER = LoggerFactory.getLogger(PushConsumerImpl.class);

    private final ClientConfiguration clientConfiguration;
    private final PushConsumerSettings pushConsumerSettings;
    private final String consumerGroup;
    private final Map<String /* topic */, FilterExpression> subscriptionExpressions;
    private final ConcurrentMap<String /* topic */, Assignments> cacheAssignments;
    private final MessageListener messageListener;
    private final int maxCacheMessageCount;
    private final int maxCacheMessageSizeInBytes;

    /**
     * Indicates the times of message reception.
     */
    private final AtomicLong receptionTimes;
    /**
     * Indicates the quantity of received messages.
     */
    private final AtomicLong receivedMessagesQuantity;

    final AtomicLong consumptionOkQuantity;
    final AtomicLong consumptionErrorQuantity;

    private final ThreadPoolExecutor consumptionExecutor;
    private final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;
    private ConsumeService consumeService;

    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    public PushConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup,
        Map<String, FilterExpression> subscriptionExpressions, MessageListener messageListener,
        int maxCacheMessageCount, int maxCacheMessageSizeInBytes, int consumptionThreadCount) {
        super(clientConfiguration, consumerGroup, subscriptionExpressions.keySet());
        this.clientConfiguration = clientConfiguration;
        Resource groupResource = new Resource(consumerGroup);
        this.pushConsumerSettings = new PushConsumerSettings(clientId, accessEndpoints, groupResource,
            clientConfiguration.getRequestTimeout(), subscriptionExpressions);
        this.consumerGroup = consumerGroup;
        this.subscriptionExpressions = subscriptionExpressions;
        this.cacheAssignments = new ConcurrentHashMap<>();
        this.messageListener = messageListener;
        this.maxCacheMessageCount = maxCacheMessageCount;
        this.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;

        this.receptionTimes = new AtomicLong(0);
        this.receivedMessagesQuantity = new AtomicLong(0);
        this.consumptionOkQuantity = new AtomicLong(0);
        this.consumptionErrorQuantity = new AtomicLong(0);

        this.processQueueTable = new ConcurrentHashMap<>();
        this.consumptionExecutor = new ThreadPoolExecutor(
            consumptionThreadCount,
            consumptionThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("MessageConsumption"));
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.info("Begin to start the rocketmq push consumer, clientId={}", clientId);
        super.startUp();
        messageMeter.setMessageCacheObserver(this);
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        this.consumeService = createConsumeService();
        // Scan assignments periodically.
        scanAssignmentsFuture = scheduler.scheduleWithFixedDelay(() -> {
            try {
                scanAssignments();
            } catch (Throwable t) {
                LOGGER.error("Exception raised while scanning the load assignments, clientId={}", clientId, t);
            }
        }, 1, 5, TimeUnit.SECONDS);
        LOGGER.info("The rocketmq push consumer starts successfully, clientId={}", clientId);
    }

    @Override
    protected void shutDown() throws InterruptedException {
        LOGGER.info("Begin to shutdown the rocketmq push consumer, clientId={}", clientId);
        if (null != scanAssignmentsFuture) {
            scanAssignmentsFuture.cancel(false);
        }
        super.shutDown();
        consumeService.stopAsync().awaitTerminated();
        consumptionExecutor.shutdown();
        ExecutorServices.awaitTerminated(consumptionExecutor);
        LOGGER.info("Shutdown the rocketmq push consumer successfully, clientId={}", clientId);
    }

    private ConsumeService createConsumeService() {
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        if (pushConsumerSettings.isFifo()) {
            return new FifoConsumeService(clientId, processQueueTable, messageListener,
                consumptionExecutor, this, scheduler);
        }
        return new StandardConsumeService(clientId, processQueueTable, messageListener,
            consumptionExecutor, this, scheduler);
    }

    /**
     * @see PushConsumer#getConsumerGroup()
     */
    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public PushConsumerSettings getPushConsumerSettings() {
        return pushConsumerSettings;
    }

    /**
     * @see PushConsumer#getSubscriptionExpressions()
     */
    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    /**
     * @see PushConsumer#subscribe(String, FilterExpression)
     */
    @Override
    public PushConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        // Check consumer status.
        if (!this.isRunning()) {
            LOGGER.error("Unable to add subscription because push consumer is not running, state={}, clientId={}", this.state(), clientId);
            throw new IllegalStateException("Push consumer is not running now");
        }

        final ListenableFuture<TopicRouteDataResult> future = getRouteDataResult(topic);
        TopicRouteDataResult topicRouteDataResult = handleClientFuture(future);
        final Status status = topicRouteDataResult.getStatus();
        final Code code = status.getCode();
        if (Code.OK.equals(code)) {
            subscriptionExpressions.put(topic, filterExpression);
            return this;
        }
        throw new ClientException(code.getNumber(), status.getMessage());
    }

    /**
     * @see PushConsumer#unsubscribe(String)
     */
    @Override
    public PushConsumer unsubscribe(String topic) {
        // Check consumer status.
        if (!this.isRunning()) {
            LOGGER.error("Unable to remove subscription because push consumer is not running, state={}, clientId={}", this.state(), clientId);
            throw new IllegalStateException("Push consumer is not running now");
        }
        subscriptionExpressions.remove(topic);
        return this;
    }

    private ListenableFuture<Endpoints> pickEndpointsToQueryAssignments(String topic) {
        final ListenableFuture<TopicRouteDataResult> future = getRouteDataResult(topic);
        return Futures.transformAsync(future, topicRouteDataResult -> {
            final SettableFuture<Endpoints> future0 = SettableFuture.create();
            final Status status = topicRouteDataResult.getStatus();
            final Code code = status.getCode();
            if (!Code.OK.equals(code)) {
                throw new ClientException(code.getNumber(), status.getMessage());
            }
            Endpoints endpoints = topicRouteDataResult.getTopicRouteData().pickEndpointsToQueryAssignments();
            future0.set(endpoints);
            return future0;
        }, MoreExecutors.directExecutor());
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic) {
        apache.rocketmq.v2.Resource topicResource = apache.rocketmq.v2.Resource.newBuilder().setName(topic).build();
        return QueryAssignmentRequest.newBuilder().setTopic(topicResource)
            .setEndpoints(accessEndpoints.toProtobuf()).setGroup(getProtobufGroup()).build();
    }

    private ListenableFuture<Assignments> queryAssignment(final String topic) {
        final ListenableFuture<Endpoints> future = pickEndpointsToQueryAssignments(topic);
        final ListenableFuture<QueryAssignmentResponse> responseFuture =
            Futures.transformAsync(future, endpoints -> {
                final Metadata metadata = sign();
                final QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);
                return clientManager.queryAssignment(endpoints, metadata, request,
                    clientConfiguration.getRequestTimeout());
            }, MoreExecutors.directExecutor());
        return Futures.transformAsync(responseFuture, response -> {
            final Status status = response.getStatus();
            final Code code = status.getCode();
            if (!Code.OK.equals(code)) {
                final String message = String.format("Failed to query assignment, code=%d, status message=[{%s}]", code.getNumber(), status.getMessage());
                throw new RuntimeException(message);
            }
            SettableFuture<Assignments> future0 = SettableFuture.create();
            final List<Assignment> assignmentList = response.getAssignmentsList().stream().map(assignment ->
                new Assignment(new MessageQueueImpl(assignment.getMessageQueue()))).collect(Collectors.toList());
            final Assignments assignments = new Assignments(assignmentList);
            future0.set(assignments);
            return future0;
        }, MoreExecutors.directExecutor());
    }

    /**
     * Drop {@link ProcessQueue} by {@link MessageQueueImpl}, {@link ProcessQueue} must be removed before it is dropped.
     *
     * @param mq message queue.
     */
    void dropProcessQueue(MessageQueueImpl mq) {
        final ProcessQueue pq = processQueueTable.remove(mq);
        if (null != pq) {
            pq.drop();
        }
    }

    /**
     * Create process queue and add it into {@link #processQueueTable}, return {@link Optional#empty()} if mapped
     * process queue already exists.
     * <p>
     * This function and {@link #dropProcessQueue(MessageQueueImpl)} make sures that process queue is not dropped if
     * it is contained in {@link #processQueueTable}, once process queue is dropped, it must have been removed
     * from {@link #processQueueTable}.
     *
     * @param mq               message queue.
     * @param filterExpression filter expression of topic.
     * @return optional process queue.
     */
    private Optional<ProcessQueue> createProcessQueue(MessageQueueImpl mq, final FilterExpression filterExpression) {
        final ProcessQueueImpl processQueue = new ProcessQueueImpl(this, mq, filterExpression);
        final ProcessQueue previous = processQueueTable.putIfAbsent(mq, processQueue);
        if (null != previous) {
            return Optional.empty();
        }
        return Optional.of(processQueue);
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup()).build();
    }

    private void syncProcessQueue(String topic, Assignments assignments, FilterExpression filterExpression) {
        Set<MessageQueueImpl> latest = new HashSet<>();

        final List<Assignment> assignmentList = assignments.getAssignmentList();
        for (Assignment assignment : assignmentList) {
            latest.add(assignment.getMessageQueue());
        }

        Set<MessageQueueImpl> activeMqs = new HashSet<>();

        for (Map.Entry<MessageQueueImpl, ProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueueImpl mq = entry.getKey();
            final ProcessQueue pq = entry.getValue();
            if (!topic.equals(mq.getTopic())) {
                continue;
            }

            if (!latest.contains(mq)) {
                LOGGER.info("Drop message queue according to the latest assignmentList, mq={}, clientId={}", mq,
                    clientId);
                dropProcessQueue(mq);
                continue;
            }

            if (pq.expired()) {
                LOGGER.warn("Drop message queue because it is expired, mq={}, clientId={}", mq, clientId);
                dropProcessQueue(mq);
                continue;
            }
            activeMqs.add(mq);
        }

        for (MessageQueueImpl mq : latest) {
            if (activeMqs.contains(mq)) {
                continue;
            }
            final Optional<ProcessQueue> optionalProcessQueue = createProcessQueue(mq, filterExpression);
            if (optionalProcessQueue.isPresent()) {
                LOGGER.info("Start to fetch message from remote, mq={}, clientId={}", mq, clientId);
                optionalProcessQueue.get().fetchMessageImmediately();
            }
        }
    }

    public void scanAssignments() {
        try {
            LOGGER.debug("Start to scan assignments periodically, clientId={}", clientId);
            for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();
                final Assignments existed = cacheAssignments.get(topic);
                final ListenableFuture<Assignments> future = queryAssignment(topic);
                Futures.addCallback(future, new FutureCallback<Assignments>() {
                    @Override
                    public void onSuccess(Assignments latest) {
                        if (latest.getAssignmentList().isEmpty()) {
                            if (null == existed || existed.getAssignmentList().isEmpty()) {
                                LOGGER.info("Acquired empty assignments from remote, would scan later, topic={}, clientId={}", topic, clientId);
                                return;
                            }
                            LOGGER.info("Attention!!! acquired empty assignments from remote, but existed assignments is not empty, topic={}, clientId={}", topic, clientId);
                        }

                        if (!latest.equals(existed)) {
                            LOGGER.info("Assignments of topic={} has changed, {} => {}, clientId={}", topic, existed, latest, clientId);
                            syncProcessQueue(topic, latest, filterExpression);
                            cacheAssignments.put(topic, latest);
                            return;
                        }
                        // Process queue may be dropped, need to be synchronized anyway.
                        syncProcessQueue(topic, latest, filterExpression);
                    }

                    @SuppressWarnings("NullableProblems")
                    @Override
                    public void onFailure(Throwable t) {
                        LOGGER.error("Exception raised while scanning the assignments, topic={}, clientId={}", topic, clientId, t);
                    }
                }, MoreExecutors.directExecutor());
            }
        } catch (Throwable t) {
            LOGGER.error("Exception raised while scanning the assignments for all topics, clientId={}", clientId, t);
        }
    }

    @Override
    public ClientSettings getClientSettings() {
        return pushConsumerSettings;
    }

    /**
     * @see PushConsumer#close()
     */
    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    int getQueueSize() {
        return processQueueTable.size();
    }

    int cacheMessageBytesThresholdPerQueue() {
        final int size = this.getQueueSize();
        // ALl process queues are removed, no need to cache messages.
        if (size <= 0) {
            return 0;
        }
        return Math.max(1, maxCacheMessageSizeInBytes / size);
    }

    int cacheMessageCountThresholdPerQueue() {
        final int size = this.getQueueSize();
        // All process queues are removed, no need to cache messages.
        if (size <= 0) {
            return 0;
        }
        return Math.max(1, maxCacheMessageCount / size);
    }

    public AtomicLong getReceptionTimes() {
        return receptionTimes;
    }

    public AtomicLong getReceivedMessagesQuantity() {
        return receivedMessagesQuantity;
    }

    public ConsumeService getConsumeService() {
        return consumeService;
    }

    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand) {
        final String nonce = verifyMessageCommand.getNonce();
        final MessageQueueImpl mq = new MessageQueueImpl(verifyMessageCommand.getMessageQueue());
        final MessageViewImpl messageView = MessageViewImpl.fromProtobuf(verifyMessageCommand.getMessage(), mq);
        final MessageId messageId = messageView.getMessageId();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView);
        Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
            @Override
            public void onSuccess(ConsumeResult consumeResult) {
                Code code = ConsumeResult.OK.equals(consumeResult) ? Code.OK : Code.FAILED_TO_CONSUME_MESSAGE;
                Status status = Status.newBuilder().setCode(code).build();
                final VerifyMessageResult verifyMessageResult = VerifyMessageResult.newBuilder().setNonce(nonce).build();
                TelemetryCommand command = TelemetryCommand.newBuilder()
                    .setVerifyMessageResult(verifyMessageResult)
                    .setStatus(status)
                    .build();
                try {
                    telemeter(endpoints, command);
                } catch (Throwable t) {
                    LOGGER.error("Failed to send message verification result command, endpoints={}, command={}, messageId={}, clientId={}", endpoints, command, messageId, clientId, t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                // Should never reach here.
                LOGGER.error("[Bug] Failed to get message verification result, endpoints={}, messageId={}, clientId={}", endpoints, messageId, clientId, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private ForwardMessageToDeadLetterQueueRequest wrapForwardMessageToDeadLetterQueueRequest(
        MessageViewImpl messageView) {
        final apache.rocketmq.v2.Resource topicResource =
            apache.rocketmq.v2.Resource.newBuilder().setName(messageView.getTopic()).build();
        return ForwardMessageToDeadLetterQueueRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .setReceiptHandle(messageView.getReceiptHandle())
            .setMessageId(messageView.getMessageId().toString())
            .setDeliveryAttempt(messageView.getDeliveryAttempt())
            .setMaxDeliveryAttempts(getRetryPolicy().getMaxAttempts()).build();
    }

    public ListenableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(
        final MessageViewImpl messageView) {
        // Intercept before forwarding message to DLQ.
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final List<MessageCommon> messageCommons = Collections.singletonList(messageView.getMessageCommon());
        doBefore(MessageHookPoints.FORWARD_TO_DLQ, messageCommons);

        final Endpoints endpoints = messageView.getEndpoints();
        ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future;
        try {
            final ForwardMessageToDeadLetterQueueRequest request =
                wrapForwardMessageToDeadLetterQueueRequest(messageView);
            final Metadata metadata = sign();
            future = clientManager.forwardMessageToDeadLetterQueue(endpoints, metadata, request,
                clientConfiguration.getRequestTimeout());
        } catch (Throwable t) {
            final SettableFuture<ForwardMessageToDeadLetterQueueResponse> future0 = SettableFuture.create();
            future0.setException(t);
            future = future0;
        }
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final Duration duration = stopwatch.elapsed();
                MessageHookPointsStatus messageHookPointsStatus = Code.OK.equals(response.getStatus().getCode()) ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                // Intercept after forwarding message to DLQ.
                doAfter(MessageHookPoints.FORWARD_TO_DLQ, messageCommons, duration, messageHookPointsStatus);
            }

            @Override
            public void onFailure(Throwable t) {
                // Intercept after forwarding message to DLQ.
                final Duration duration = stopwatch.elapsed();
                doAfter(MessageHookPoints.FORWARD_TO_DLQ, messageCommons, duration, MessageHookPointsStatus.ERROR);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @Override
    public void doStats() {
        final long receptionTimes = this.receptionTimes.getAndSet(0);
        final long receivedMessagesQuantity = this.receivedMessagesQuantity.getAndSet(0);

        final long consumptionOkQuantity = this.consumptionOkQuantity.getAndSet(0);
        final long consumptionErrorQuantity = this.consumptionErrorQuantity.getAndSet(0);

        LOGGER.info("clientId={}, consumerGroup={}, receptionTimes={}, receivedMessagesQuantity={}, consumptionOkQuantity={}, consumptionErrorQuantity={}",
            clientId, consumerGroup, receptionTimes, receivedMessagesQuantity, consumptionOkQuantity, consumptionErrorQuantity);
        for (ProcessQueue pq : processQueueTable.values()) {
            LOGGER.info("Process queue stats: clientId={}, mq={}, pendingMessageCount={}, inflightMessageCount={}, cachedMessageBytes={}",
                clientId, pq.getMessageQueue(), pq.getPendingMessageCount(), pq.getInflightMessageCount(), pq.getCachedMessageBytes());
        }
    }

    public RetryPolicy getRetryPolicy() {
        return pushConsumerSettings.getRetryPolicy();
    }

    public ThreadPoolExecutor getConsumptionExecutor() {
        return consumptionExecutor;
    }

    @Override
    public Map<String, Long> getCachedMessageCount() {
        Map<String, Long> cachedMessageCountMap = new HashMap<>();
        for (ProcessQueue pq : processQueueTable.values()) {
            final String topic = pq.getMessageQueue().getTopic();
            long count = cachedMessageCountMap.containsKey(topic) ? cachedMessageCountMap.get(topic) : 0;
            count += pq.getInflightMessageCount();
            count += pq.getPendingMessageCount();
            cachedMessageCountMap.put(topic, count);
        }
        return cachedMessageCountMap;
    }

    @Override
    public Map<String, Long> getCachedMessageBytes() {
        Map<String, Long> cachedMessageBytesMap = new HashMap<>();
        for (ProcessQueue pq : processQueueTable.values()) {
            final String topic = pq.getMessageQueue().getTopic();
            long bytes = cachedMessageBytesMap.containsKey(topic) ? cachedMessageBytesMap.get(topic) : 0;
            bytes += pq.getCachedMessageBytes();
            cachedMessageBytesMap.put(topic, bytes);
        }
        return cachedMessageBytesMap;
    }
}
