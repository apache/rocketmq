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
import java.util.LinkedHashMap;
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
import org.apache.rocketmq.proxy.common.BatchChangeInvisibleTimeResult;
import org.apache.rocketmq.proxy.common.BatchRenewEvent;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroupKey;
import org.apache.rocketmq.proxy.common.RenewEvent;
import org.apache.rocketmq.proxy.common.RenewStrategyPolicy;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.common.utils.BatchChangeInvisibleTimeUtils;
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
    private final StateEventListener<BatchRenewEvent> batchEventListener;
    protected final static RetryPolicy RENEW_POLICY = new RenewStrategyPolicy();
    protected final ScheduledExecutorService scheduledExecutorService =
        ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RenewalScheduledThread_"));
    protected final ThreadPoolExecutor renewalWorkerService;
    protected final ThreadPoolExecutor returnHandleGroupWorkerService;

    public DefaultReceiptHandleManager(MetadataService metadataService, ConsumerManager consumerManager, StateEventListener<RenewEvent> eventListener) {
        this(metadataService, consumerManager, eventListener, null);
    }

    public DefaultReceiptHandleManager(MetadataService metadataService, ConsumerManager consumerManager,
        StateEventListener<RenewEvent> eventListener, StateEventListener<BatchRenewEvent> batchEventListener) {
        this.batchEventListener = batchEventListener;
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
                if (batchEventListener != null && proxyConfig.isEnableBatchChangeInvisibleTime()) {
                    List<RenewMessage> renewMessageList = new ArrayList<>();
                    group.scan((msgID, handleStr, v) -> {
                        long current = System.currentTimeMillis();
                        ReceiptHandle handle = ReceiptHandle.decode(v.getReceiptHandleStr());
                        if (handle.getNextVisibleTime() - current <= proxyConfig.getRenewAheadTimeMillis()) {
                            renewMessageList.add(new RenewMessage(msgID, handleStr, v));
                        }
                    });
                    if (!renewMessageList.isEmpty()) {
                        renewalWorkerService.submit(() -> renewMessageBatch(createContext("RenewMessage"), key, group,
                            renewMessageList));
                    }
                    continue;
                }
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
        RenewEventData renewEventData = prepareRenewMessage(context, key, messageReceiptHandle);
        if (renewEventData.getEventType() != null) {
            CompletableFuture<AckResult> future = new CompletableFuture<>();
            future.whenComplete((result, throwable) -> completeRenewMessage(renewEventData, result, throwable));
            fireEvent(new RenewEvent(key, messageReceiptHandle, renewEventData.getRenewTime(),
                renewEventData.getEventType(), future));
        }
        return renewEventData.getResultFuture();
    }

    protected void renewMessageBatch(ProxyContext context, ReceiptHandleGroupKey key, ReceiptHandleGroup group,
        List<RenewMessage> renewMessageList) {
        Map<List<String>, List<RenewMessage>> brokerBatches = new LinkedHashMap<>();
        for (RenewMessage message : renewMessageList) {
            brokerBatches.computeIfAbsent(message.batchKey, ignored -> new ArrayList<>()).add(message);
        }
        int batchMaxNum = Math.max(1, ConfigurationManager.getProxyConfig().getBatchChangeInvisibleTimeMaxNum());
        for (List<RenewMessage> messages : brokerBatches.values()) {
            BatchChangeInvisibleTimeUtils.sendBatches(messages, batchMaxNum,
                batch -> renewBatch(context, key, group, batch));
        }
    }

    private CompletableFuture<Void> renewBatch(ProxyContext context, ReceiptHandleGroupKey key,
        ReceiptHandleGroup group, List<RenewMessage> batch) {
        List<RenewEventData> eventDataList = new ArrayList<>(batch.size());
        for (RenewMessage renewMessage : batch) {
            try {
                group.computeIfPresent(renewMessage.getMsgID(), renewMessage.getHandleStr(), messageReceiptHandle -> {
                    ReceiptHandle handle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
                    if (handle.getNextVisibleTime() - System.currentTimeMillis()
                        > ConfigurationManager.getProxyConfig().getRenewAheadTimeMillis()) {
                        return CompletableFuture.completedFuture(messageReceiptHandle);
                    }
                    RenewEventData data = prepareRenewMessage(context, key, messageReceiptHandle);
                    if (data.getEventType() != null) {
                        eventDataList.add(data);
                    }
                    return data.getResultFuture();
                }, 0);
            } catch (Exception e) {
                log.error("error when renew message. msgID:{}, handleStr:{}", renewMessage.getMsgID(), renewMessage.getHandleStr(), e);
            }
        }
        return fireRenewEvent(key, eventDataList);
    }

    private CompletableFuture<Void> fireRenewEvent(ReceiptHandleGroupKey key, List<RenewEventData> eventDataList) {
        if (eventDataList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        BatchRenewEvent event = new BatchRenewEvent(key, eventDataList, eventDataList.get(0).getEventType());
        CompletableFuture<Void> completion = event.getFuture().handle((results, throwable) -> {
            for (int i = 0; i < eventDataList.size(); i++) {
                BatchChangeInvisibleTimeResult result = results != null && i < results.size() ? results.get(i) : null;
                Throwable error = throwable;
                if (error == null && (result == null || result.getAckResult() == null)) {
                    error = result != null && result.getProxyException() != null ? result.getProxyException()
                        : new IllegalStateException("batch change invisible time result missing");
                }
                completeRenewMessage(eventDataList.get(i), result == null ? null : result.getAckResult(), error);
            }
            return null;
        });
        try {
            batchEventListener.fireEvent(event);
        } catch (Throwable t) {
            event.getFuture().completeExceptionally(t);
        }
        return completion;
    }

    private void fireEvent(RenewEvent event) {
        try {
            eventListener.fireEvent(event);
        } catch (Throwable t) {
            event.getFuture().completeExceptionally(t);
        }
    }

    private void completeRenewMessage(RenewEventData data, AckResult result, Throwable throwable) {
        MessageReceiptHandle message = data.getMessageReceiptHandle();
        try {
            if (data.getEventType() == RenewEvent.EventType.STOP_RENEW) {
                if (throwable != null) {
                    log.error("error when nack in renew. handle:{}", message, throwable);
                }
                data.getResultFuture().complete(null);
            } else if (throwable != null) {
                log.error("error when renew. handle:{}", message, throwable);
                if (renewExceptionNeedRetry(throwable)) {
                    message.incrementAndGetRenewRetryTimes();
                    data.getResultFuture().complete(message);
                } else {
                    data.getResultFuture().complete(null);
                }
            } else if (AckStatus.OK.equals(result.getStatus())) {
                message.updateReceiptHandle(result.getExtraInfo());
                message.resetRenewRetryTimes();
                message.incrementRenewTimes();
                data.getResultFuture().complete(message);
            } else {
                log.error("renew response is not ok. result:{}, handle:{}", result, message);
                data.getResultFuture().complete(null);
            }
        } catch (Throwable t) {
            // A malformed result must not leave this handle, or the rest of the batch, locked.
            data.getResultFuture().completeExceptionally(t);
        }
    }

    protected RenewEventData prepareRenewMessage(ProxyContext context, ReceiptHandleGroupKey key,
        MessageReceiptHandle messageReceiptHandle) {
        CompletableFuture<MessageReceiptHandle> resFuture = new CompletableFuture<>();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        long current = System.currentTimeMillis();
        try {
            if (messageReceiptHandle.getRenewRetryTimes() >= proxyConfig.getMaxRenewRetryTimes()) {
                log.warn("handle has exceed max renewRetryTimes. handle:{}", messageReceiptHandle);
                return RenewEventData.completed(messageReceiptHandle, resFuture, null);
            }
            if (current - messageReceiptHandle.getConsumeTimestamp() < proxyConfig.getRenewMaxTimeMillis()) {
                return new RenewEventData(messageReceiptHandle, RENEW_POLICY.nextDelayDuration(messageReceiptHandle.getRenewTimes()),
                    RenewEvent.EventType.RENEW, resFuture);
            } else {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    metadataService.getSubscriptionGroupConfig(context, messageReceiptHandle.getGroup());
                if (subscriptionGroupConfig == null) {
                    log.error("group's subscriptionGroupConfig is null when renew. handle: {}", messageReceiptHandle);
                    return RenewEventData.completed(messageReceiptHandle, resFuture, null);
                }
                RetryPolicy retryPolicy = subscriptionGroupConfig.getGroupRetryPolicy().getRetryPolicy();
                return new RenewEventData(messageReceiptHandle, retryPolicy.nextDelayDuration(messageReceiptHandle.getReconsumeTimes()),
                    RenewEvent.EventType.STOP_RENEW, resFuture);
            }
        } catch (Throwable t) {
            log.error("unexpect error when renew message, stop to renew it. handle:{}", messageReceiptHandle, t);
            resFuture.complete(null);
        }
        return RenewEventData.completed(messageReceiptHandle, resFuture, null);
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
        if (batchEventListener != null && proxyConfig.isEnableBatchChangeInvisibleTime()) {
            fireClearGroupEventBatch(key, handleGroup, proxyConfig);
        } else {
            handleGroup.scan((msgId, handle, v) -> {
                try {
                    handleGroup.computeIfPresent(msgId, handle, messageReceiptHandle -> {
                        fireEvent(new RenewEvent(key, messageReceiptHandle,
                            proxyConfig.getInvisibleTimeMillisWhenClear(), RenewEvent.EventType.CLEAR_GROUP, new CompletableFuture<>()));
                        return CompletableFuture.completedFuture(null);
                    }, 0);
                } catch (Exception e) {
                    log.error("error when clear handle for group. key:{}", key, e);
                }
            });
        }
        // scheduleRenewTask will trigger cleanup again
        if (!handleGroup.isEmpty()) {
            log.warn("The handle cannot be completely cleared, the remaining quantity is {}, key:{}", handleGroup.getHandleNum(), key);
            receiptHandleGroupMap.putIfAbsent(key, handleGroup);
        }
    }

    protected void fireClearGroupEventBatch(ReceiptHandleGroupKey key, ReceiptHandleGroup handleGroup,
        ProxyConfig proxyConfig) {
        Map<List<String>, List<BatchRenewEvent.Entry>> brokerBatches = new LinkedHashMap<>();
        handleGroup.scan((msgID, handle, v) -> {
            try {
                handleGroup.computeIfPresent(msgID, handle, message -> {
                    ReceiptHandle receipt = ReceiptHandle.decode(message.getReceiptHandleStr());
                    brokerBatches.computeIfAbsent(BatchChangeInvisibleTimeUtils.batchKey(receipt,
                        message.getGroup(), message.getTopic()), ignored -> new ArrayList<>())
                        .add(new BatchRenewEvent.Entry(message, proxyConfig.getInvisibleTimeMillisWhenClear()));
                    return CompletableFuture.completedFuture(null);
                }, 0);
            } catch (Exception e) {
                log.error("error when clear handle for group. key:{}", key, e);
            }
        });
        int batchMaxNum = Math.max(1, proxyConfig.getBatchChangeInvisibleTimeMaxNum());
        for (List<BatchRenewEvent.Entry> messages : brokerBatches.values()) {
            BatchChangeInvisibleTimeUtils.sendBatches(messages, batchMaxNum, batch -> {
                BatchRenewEvent event = new BatchRenewEvent(key, batch, RenewEvent.EventType.CLEAR_GROUP);
                batchEventListener.fireEvent(event);
                return event.getFuture();
            });
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

    protected static class RenewMessage {
        private final String msgID;
        private final String handleStr;
        private final List<String> batchKey;

        public RenewMessage(String msgID, String handleStr, MessageReceiptHandle message) {
            this.msgID = msgID;
            this.handleStr = handleStr;
            this.batchKey = BatchChangeInvisibleTimeUtils.batchKey(message.getOriginalReceiptHandle(),
                message.getGroup(), message.getTopic());
        }

        public String getMsgID() {
            return msgID;
        }

        public String getHandleStr() {
            return handleStr;
        }
    }

    protected static class RenewEventData extends BatchRenewEvent.Entry {
        private final RenewEvent.EventType eventType;
        private final CompletableFuture<MessageReceiptHandle> resultFuture;

        public RenewEventData(MessageReceiptHandle messageReceiptHandle, long renewTime,
            RenewEvent.EventType eventType,
            CompletableFuture<MessageReceiptHandle> resultFuture) {
            super(messageReceiptHandle, renewTime);
            this.eventType = eventType;
            this.resultFuture = resultFuture;
        }

        public static RenewEventData completed(MessageReceiptHandle messageReceiptHandle,
            CompletableFuture<MessageReceiptHandle> resultFuture, MessageReceiptHandle result) {
            resultFuture.complete(result);
            return new RenewEventData(messageReceiptHandle, 0, null, resultFuture);
        }

        public RenewEvent.EventType getEventType() {
            return eventType;
        }

        public CompletableFuture<MessageReceiptHandle> getResultFuture() {
            return resultFuture;
        }
    }
}
