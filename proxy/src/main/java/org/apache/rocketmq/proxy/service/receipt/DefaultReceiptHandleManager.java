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

package org.apache.rocketmq.proxy.service.receipt;

import com.google.common.base.Stopwatch;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.state.StateEventListener;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.BatchConsumeGroupSummary;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.PopReceiptHandleGroupSummary;
import org.apache.rocketmq.proxy.common.PopReceiptHandleInfo;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroupKey;
import org.apache.rocketmq.proxy.common.RenewEvent;
import org.apache.rocketmq.proxy.common.RenewStrategyPolicy;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.remoting.protocol.subscription.RetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class DefaultReceiptHandleManager extends AbstractStartAndShutdown implements ReceiptHandleManager {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final MetadataService metadataService;
    protected final ConsumerManager consumerManager;
    protected final ConcurrentMap<ReceiptHandleGroupKey, ReceiptHandleGroup> receiptHandleGroupMap;
    protected final StateEventListener<RenewEvent> eventListener;
    protected final static RetryPolicy RENEW_POLICY = new RenewStrategyPolicy();
    protected final ScheduledExecutorService scheduledExecutorService =
        ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RenewalScheduledThread_"));
    protected final ThreadPoolExecutor renewalWorkerService;
    protected final ThreadPoolExecutor returnHandleGroupWorkerService;

    public DefaultReceiptHandleManager(MetadataService metadataService, ConsumerManager consumerManager, StateEventListener<RenewEvent> eventListener) {
        this.metadataService = metadataService;
        this.consumerManager = consumerManager;
        this.eventListener = eventListener;
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.renewalWorkerService = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getRenewThreadPoolNums(),
            proxyConfig.getRenewMaxThreadPoolNums(),
            1, TimeUnit.MINUTES,
            "RenewalWorkerThread",
            proxyConfig.getRenewThreadPoolQueueCapacity()
        );
        this.returnHandleGroupWorkerService = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getReturnHandleGroupThreadPoolNums(),
            proxyConfig.getReturnHandleGroupThreadPoolNums() * 2,
            1, TimeUnit.MINUTES,
            "ReturnHandleGroupWorkerThread",
            proxyConfig.getRenewThreadPoolQueueCapacity()
        );
        consumerManager.appendConsumerIdsChangeListener(new ConsumerIdsChangeListener() {
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                if (ConsumerGroupEvent.CLIENT_UNREGISTER.equals(event)) {
                    if (args == null || args.length < 1) {
                        return;
                    }
                    if (args[0] instanceof ClientChannelInfo) {
                        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                        if (ChannelHelper.isRemote(clientChannelInfo.getChannel())) {
                            // if the channel sync from other proxy is expired, not to clear data of connect to current proxy
                            return;
                        }
                        clearGroup(new ReceiptHandleGroupKey(clientChannelInfo.getChannel(), group));
                        log.info("clear handle of this client when client unregister. group:{}, clientChannelInfo:{}", group, clientChannelInfo);
                    }
                }
            }

            @Override
            public void shutdown() {

            }
        });
        this.receiptHandleGroupMap = new ConcurrentHashMap<>();
        this.renewalWorkerService.setRejectedExecutionHandler((r, executor) -> log.warn("add renew task failed. queueSize:{}", executor.getQueue().size()));
        this.appendStartAndShutdown(new StartAndShutdown() {
            @Override
            public void start() throws Exception {
                scheduledExecutorService.scheduleWithFixedDelay(() -> scheduleRenewTask(), 0,
                    ConfigurationManager.getProxyConfig().getRenewSchedulePeriodMillis(), TimeUnit.MILLISECONDS);
            }

            @Override
            public void shutdown() throws Exception {
                scheduledExecutorService.shutdown();
                clearAllHandle();
            }
        });
    }

    public void addReceiptHandle(ProxyContext context, Channel channel, String group, String msgID, MessageReceiptHandle messageReceiptHandle) {
        ConcurrentHashMapUtils.computeIfAbsent(this.receiptHandleGroupMap, new ReceiptHandleGroupKey(channel, group),
            k -> new ReceiptHandleGroup()).put(msgID, messageReceiptHandle);
    }

    public MessageReceiptHandle removeReceiptHandle(ProxyContext context, Channel channel, String group, String msgID, String receiptHandle) {
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.get(new ReceiptHandleGroupKey(channel, group));
        if (handleGroup == null) {
            return null;
        }
        return handleGroup.remove(msgID, receiptHandle);
    }

    public int getUnackedMessageCount(ProxyContext context, Channel channel, String group) {
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.get(new ReceiptHandleGroupKey(channel, group));
        return handleGroup == null ? 0 : handleGroup.getMsgCount();
    }

    protected boolean clientIsOffline(ReceiptHandleGroupKey groupKey) {
        return this.consumerManager.findChannel(groupKey.getGroup(), groupKey.getChannel()) == null;
    }

    protected void scheduleRenewTask() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
            for (Map.Entry<ReceiptHandleGroupKey, ReceiptHandleGroup> entry : receiptHandleGroupMap.entrySet()) {
                ReceiptHandleGroupKey key = entry.getKey();
                if (clientIsOffline(key)) {
                    clearGroup(key);
                    continue;
                }

                ReceiptHandleGroup group = entry.getValue();
                group.scan((msgID, handleStr, v) -> {
                    long current = System.currentTimeMillis();
                    ReceiptHandle handle = ReceiptHandle.decode(v.getReceiptHandleStr());
                    if (handle.getNextVisibleTime() - current > proxyConfig.getRenewAheadTimeMillis()) {
                        return;
                    }
                    renewalWorkerService.submit(() -> renewMessage(createContext("RenewMessage"), key, group,
                        msgID, handleStr));
                });
            }
        } catch (Exception e) {
            log.error("unexpect error when schedule renew task", e);
        }

        log.debug("scan for renewal done. cost:{}ms", stopwatch.elapsed().toMillis());
    }

    protected void renewMessage(ProxyContext context, ReceiptHandleGroupKey key, ReceiptHandleGroup group, String msgID, String handleStr) {
        try {
            group.computeIfPresent(msgID, handleStr, messageReceiptHandle -> startRenewMessage(context, key, messageReceiptHandle), 0);
        } catch (Exception e) {
            log.error("error when renew message. msgID:{}, handleStr:{}", msgID, handleStr, e);
        }
    }

    protected CompletableFuture<MessageReceiptHandle> startRenewMessage(ProxyContext context, ReceiptHandleGroupKey key, MessageReceiptHandle messageReceiptHandle) {
        CompletableFuture<MessageReceiptHandle> resFuture = new CompletableFuture<>();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        long current = System.currentTimeMillis();
        try {
            if (messageReceiptHandle.getRenewRetryTimes() >= proxyConfig.getMaxRenewRetryTimes()) {
                log.warn("handle has exceed max renewRetryTimes. handle:{}", messageReceiptHandle);
                return CompletableFuture.completedFuture(null);
            }
            if (current - messageReceiptHandle.getConsumeTimestamp() < proxyConfig.getRenewMaxTimeMillis()) {
                CompletableFuture<AckResult> future = new CompletableFuture<>();
                eventListener.fireEvent(new RenewEvent(key, messageReceiptHandle, RENEW_POLICY.nextDelayDuration(messageReceiptHandle.getRenewTimes()), RenewEvent.EventType.RENEW, future));
                future.whenComplete((ackResult, throwable) -> {
                    if (throwable != null) {
                        log.error("error when renew. handle:{}", messageReceiptHandle, throwable);
                        if (renewExceptionNeedRetry(throwable)) {
                            messageReceiptHandle.incrementAndGetRenewRetryTimes();
                            resFuture.complete(messageReceiptHandle);
                        } else {
                            resFuture.complete(null);
                        }
                    } else if (AckStatus.OK.equals(ackResult.getStatus())) {
                        messageReceiptHandle.updateReceiptHandle(ackResult.getExtraInfo());
                        messageReceiptHandle.resetRenewRetryTimes();
                        messageReceiptHandle.incrementRenewTimes();
                        resFuture.complete(messageReceiptHandle);
                    } else {
                        log.error("renew response is not ok. result:{}, handle:{}", ackResult, messageReceiptHandle);
                        resFuture.complete(null);
                    }
                });
            } else {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    metadataService.getSubscriptionGroupConfig(context, messageReceiptHandle.getGroup());
                if (subscriptionGroupConfig == null) {
                    log.error("group's subscriptionGroupConfig is null when renew. handle: {}", messageReceiptHandle);
                    return CompletableFuture.completedFuture(null);
                }
                RetryPolicy retryPolicy = subscriptionGroupConfig.getGroupRetryPolicy().getRetryPolicy();
                CompletableFuture<AckResult> future = new CompletableFuture<>();
                eventListener.fireEvent(new RenewEvent(key, messageReceiptHandle, retryPolicy.nextDelayDuration(messageReceiptHandle.getReconsumeTimes()), RenewEvent.EventType.STOP_RENEW, future));
                future.whenComplete((ackResult, throwable) -> {
                    if (throwable != null) {
                        log.error("error when nack in renew. handle:{}", messageReceiptHandle, throwable);
                    }
                    resFuture.complete(null);
                });
            }
        } catch (Throwable t) {
            log.error("unexpect error when renew message, stop to renew it. handle:{}", messageReceiptHandle, t);
            resFuture.complete(null);
        }
        return resFuture;
    }

    protected void clearGroup(ReceiptHandleGroupKey key) {
        if (key == null) {
            return;
        }
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.remove(key);
        returnHandleGroupWorkerService.submit(() -> returnHandleGroup(key, handleGroup));
    }

    // There is no longer any waiting for lock, and only the locked handles will be processed immediately,
    // while the handles that cannot be acquired will be kept waiting for the next scheduling.
    private void returnHandleGroup(ReceiptHandleGroupKey key, ReceiptHandleGroup handleGroup) {
        if (handleGroup == null || handleGroup.isEmpty()) {
            return;
        }
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        handleGroup.scan((msgID, handle, v) -> {
            try {
                handleGroup.computeIfPresent(msgID, handle, messageReceiptHandle -> {
                    CompletableFuture<AckResult> future = new CompletableFuture<>();
                    eventListener.fireEvent(new RenewEvent(key, messageReceiptHandle, proxyConfig.getInvisibleTimeMillisWhenClear(), RenewEvent.EventType.CLEAR_GROUP, future));
                    return CompletableFuture.completedFuture(null);
                }, 0);
            } catch (Exception e) {
                log.error("error when clear handle for group. key:{}", key, e);
            }
        });
        // scheduleRenewTask will trigger cleanup again
        if (!handleGroup.isEmpty()) {
            log.warn("The handle cannot be completely cleared, the remaining quantity is {}, key:{}", handleGroup.getHandleNum(), key);
            receiptHandleGroupMap.putIfAbsent(key, handleGroup);
        }
    }

    protected void clearAllHandle() {
        log.info("start clear all handle in receiptHandleProcessor");
        Set<ReceiptHandleGroupKey> keySet = receiptHandleGroupMap.keySet();
        for (ReceiptHandleGroupKey key : keySet) {
            clearGroup(key);
        }
        log.info("clear all handle in receiptHandleProcessor done");
    }

    protected boolean renewExceptionNeedRetry(Throwable t) {
        t = ExceptionUtils.getRealException(t);
        if (t instanceof ProxyException) {
            ProxyException proxyException = (ProxyException) t;
            if (ProxyExceptionCode.INVALID_BROKER_NAME.equals(proxyException.getCode()) ||
                ProxyExceptionCode.INVALID_RECEIPT_HANDLE.equals(proxyException.getCode())) {
                return false;
            }
        }
        return true;
    }

    protected ProxyContext createContext(String actionName) {
        return ProxyContext.createForInner(this.getClass().getSimpleName() + actionName);
    }

    /**
     * Query POP receipt handles for diagnostics.
     * <p>
     * Scans all receipt handle groups matching the given consumer group name,
     * collects diagnostic information for each unacked message handle,
     * and returns a summary with paginated handle details.
     * <p>
     * This method is thread-safe: it iterates over the ConcurrentHashMap without
     * locking, providing a weakly consistent snapshot of the current state.
     */
    @Override
    public PopReceiptHandleDiagnosticResult describePopReceiptHandles(String group, String topic, int pageNum, int pageSize) {
        List<PopReceiptHandleInfo> allHandles = new ArrayList<>();
        long totalRenewTimes = 0;
        long totalRenewRetryTimes = 0;
        int expiredHandles = 0;
        int totalHandleCount = 0;
        int totalMessageCount = 0;

        long currentTime = System.currentTimeMillis();

        // Scan all receipt handle groups matching the group name
        for (Map.Entry<ReceiptHandleGroupKey, ReceiptHandleGroup> entry : receiptHandleGroupMap.entrySet()) {
            ReceiptHandleGroupKey key = entry.getKey();
            if (!key.getGroup().equals(group)) {
                continue;
            }

            ReceiptHandleGroup handleGroup = entry.getValue();
            totalHandleCount += (int) handleGroup.getHandleNum();
            totalMessageCount += handleGroup.getMsgCount();

            // Scan handles in this group
            handleGroup.scan((msgID, handleStr, messageReceiptHandle) -> {
                // Apply topic filter if specified
                if (topic != null && !topic.isEmpty() && !topic.equals(messageReceiptHandle.getTopic())) {
                    return;
                }

                // Decode receipt handle to get invisible time info
                ReceiptHandle receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
                boolean isExpired = receiptHandle.getNextVisibleTime() <= currentTime;

                PopReceiptHandleInfo info = new PopReceiptHandleInfo(
                    messageReceiptHandle.getGroup(),
                    messageReceiptHandle.getTopic(),
                    messageReceiptHandle.getQueueId(),
                    messageReceiptHandle.getMessageId(),
                    messageReceiptHandle.getQueueOffset(),
                    messageReceiptHandle.getReconsumeTimes(),
                    messageReceiptHandle.getRenewTimes(),
                    messageReceiptHandle.getRenewRetryTimes(),
                    messageReceiptHandle.getConsumeTimestamp(),
                    messageReceiptHandle.getReceiptHandleStr(),
                    receiptHandle.getNextVisibleTime(),
                    receiptHandle.getInvisibleTime(),
                    receiptHandle.getBrokerName(),
                    isExpired
                );
                allHandles.add(info);
            });
        }

        // Aggregate summary statistics
        for (PopReceiptHandleInfo info : allHandles) {
            totalRenewTimes += info.getRenewTimes();
            totalRenewRetryTimes += info.getRenewRetryTimes();
            if (info.isExpired()) {
                expiredHandles++;
            }
        }

        PopReceiptHandleGroupSummary summary = new PopReceiptHandleGroupSummary(
            group, totalHandleCount, totalMessageCount,
            totalRenewTimes, totalRenewRetryTimes, expiredHandles
        );

        // Apply pagination
        long total = allHandles.size();
        int fromIndex = (pageNum - 1) * pageSize;
        int toIndex = Math.min(fromIndex + pageSize, allHandles.size());
        List<PopReceiptHandleInfo> pagedHandles;
        if (fromIndex >= allHandles.size()) {
            pagedHandles = new ArrayList<>();
        } else {
            pagedHandles = allHandles.subList(fromIndex, toIndex);
        }

        return new PopReceiptHandleDiagnosticResult(summary, pagedHandles, total, pageNum, pageSize);
    }

    /**
     * Query batch consumption diagnostics, aggregated per client channel.
     * <p>
     * Scans all receipt handle groups matching the given consumer group,
     * aggregates unacked message statistics per Channel, and returns
     * a summary with paginated per-channel diagnostic data.
     * <p>
     * This method is thread-safe: it iterates over the ConcurrentHashMap without
     * locking, providing a weakly consistent snapshot of the current state.
     */
    @Override
    public BatchConsumeDiagnosticResult describeBatchConsumeDiagnostics(String group, String topic, int pageNum, int pageSize) {
        long currentTime = System.currentTimeMillis();

        // Aggregate per-channel data: Channel -> aggregated stats
        Map<Channel, ChannelAggregator> channelAggregators = new HashMap<>();

        // Scan all receipt handle groups matching the group name
        for (Map.Entry<ReceiptHandleGroupKey, ReceiptHandleGroup> entry : receiptHandleGroupMap.entrySet()) {
            ReceiptHandleGroupKey key = entry.getKey();
            if (!key.getGroup().equals(group)) {
                continue;
            }

            Channel channel = key.getChannel();
            ReceiptHandleGroup handleGroup = entry.getValue();
            ChannelAggregator aggregator = channelAggregators.computeIfAbsent(channel, ChannelAggregator::new);

            // Scan handles in this group
            handleGroup.scan((msgID, handleStr, messageReceiptHandle) -> {
                // Apply topic filter if specified
                if (topic != null && !topic.isEmpty() && !topic.equals(messageReceiptHandle.getTopic())) {
                    return;
                }

                // Decode receipt handle to check expiration
                ReceiptHandle receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
                boolean isExpired = receiptHandle.getNextVisibleTime() <= currentTime;

                aggregator.unackedMessageCount++;
                aggregator.totalRenewTimes += messageReceiptHandle.getRenewTimes();
                aggregator.totalRenewRetryTimes += messageReceiptHandle.getRenewRetryTimes();
                if (isExpired) {
                    aggregator.expiredHandleCount++;
                }
                aggregator.topicDistribution.merge(messageReceiptHandle.getTopic(), 1, Integer::sum);
            });

            // Handle count is per-group (one ReceiptHandleGroup per Channel+group key)
            aggregator.unackedHandleCount += (int) handleGroup.getHandleNum();
        }

        // Build per-channel data list
        List<ChannelBatchConsumeData> allChannelData = new ArrayList<>();
        int totalUnackedMessages = 0;
        int totalUnackedHandles = 0;
        int totalExpiredHandles = 0;
        long totalRenewTimes = 0;
        long totalRenewRetryTimes = 0;

        for (ChannelAggregator aggregator : channelAggregators.values()) {
            // Skip channels with no unacked messages (after topic filter)
            if (aggregator.unackedMessageCount == 0) {
                continue;
            }

            allChannelData.add(new ChannelBatchConsumeData(
                aggregator.channel,
                aggregator.unackedMessageCount,
                aggregator.unackedHandleCount,
                aggregator.totalRenewTimes,
                aggregator.totalRenewRetryTimes,
                aggregator.expiredHandleCount,
                new HashMap<>(aggregator.topicDistribution)
            ));

            totalUnackedMessages += aggregator.unackedMessageCount;
            totalUnackedHandles += aggregator.unackedHandleCount;
            totalExpiredHandles += aggregator.expiredHandleCount;
            totalRenewTimes += aggregator.totalRenewTimes;
            totalRenewRetryTimes += aggregator.totalRenewRetryTimes;
        }

        BatchConsumeGroupSummary summary = new BatchConsumeGroupSummary(
            group, allChannelData.size(), totalUnackedMessages,
            totalUnackedHandles, totalExpiredHandles, totalRenewTimes, totalRenewRetryTimes
        );

        // Apply pagination
        long total = allChannelData.size();
        int fromIndex = (pageNum - 1) * pageSize;
        int toIndex = Math.min(fromIndex + pageSize, allChannelData.size());
        List<ChannelBatchConsumeData> pagedData;
        if (fromIndex >= allChannelData.size()) {
            pagedData = new ArrayList<>();
        } else {
            pagedData = allChannelData.subList(fromIndex, toIndex);
        }

        return new BatchConsumeDiagnosticResult(summary, pagedData, total, pageNum, pageSize);
    }

    /**
     * Internal aggregator for per-channel statistics during batch diagnostics scan.
     */
    private static class ChannelAggregator {
        final Channel channel;
        int unackedMessageCount = 0;
        int unackedHandleCount = 0;
        long totalRenewTimes = 0;
        long totalRenewRetryTimes = 0;
        int expiredHandleCount = 0;
        final Map<String, Integer> topicDistribution = new HashMap<>();

        ChannelAggregator(Channel channel) {
            this.channel = channel;
        }
    }
}
