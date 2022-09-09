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

package org.apache.rocketmq.proxy.processor;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Stopwatch;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.subscription.RetryPolicy;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiptHandleProcessor extends AbstractStartAndShutdown {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final ConcurrentMap<ReceiptHandleGroupKey, ReceiptHandleGroup> receiptHandleGroupMap;
    protected final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RenewalScheduledThread_"));
    protected ThreadPoolExecutor renewalWorkerService;
    protected final MessagingProcessor messagingProcessor;

    public ReceiptHandleProcessor(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
        this.receiptHandleGroupMap = new ConcurrentHashMap<>();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.renewalWorkerService = ThreadPoolMonitor.createAndMonitor(
            proxyConfig.getRenewThreadPoolNums(),
            proxyConfig.getRenewMaxThreadPoolNums(),
            1, TimeUnit.MINUTES,
            "RenewalWorkerThread",
            proxyConfig.getRenewThreadPoolQueueCapacity()
        );
        this.init();
    }

    protected void init() {
        this.registerConsumerListener();
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

    protected void registerConsumerListener() {
        this.messagingProcessor.registerConsumerListener(new ConsumerIdsChangeListener() {
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                if (ConsumerGroupEvent.CLIENT_UNREGISTER.equals(event)) {
                    if (args == null || args.length < 1) {
                        return;
                    }
                    if (args[0] instanceof ClientChannelInfo) {
                        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                        clearGroup(new ReceiptHandleGroupKey(clientChannelInfo.getClientId(), group));
                    }
                }
            }

            @Override
            public void shutdown() {

            }
        });
    }

    protected ProxyContext createContext(String actionName) {
        return ProxyContext.createForInner(this.getClass().getSimpleName() + actionName);
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
                    renewalWorkerService.submit(() -> renewMessage(group, msgID, handleStr));
                });
            }
        } catch (Exception e) {
            log.error("unexpect error when schedule renew task", e);
        }

        log.info("scan for renewal done. cost:{}ms", stopwatch.elapsed().toMillis());
    }

    protected void renewMessage(ReceiptHandleGroup group, String msgID, String handleStr) {
        try {
            group.computeIfPresent(msgID, handleStr, this::startRenewMessage);
        } catch (Exception e) {
            log.error("error when renew message. msgID:{}, handleStr:{}", msgID, handleStr, e);
        }
    }

    protected CompletableFuture<MessageReceiptHandle> startRenewMessage(MessageReceiptHandle messageReceiptHandle) {
        CompletableFuture<MessageReceiptHandle> resFuture = new CompletableFuture<>();
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        ProxyContext context = createContext("RenewMessage");
        ReceiptHandle handle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
        long current = System.currentTimeMillis();
        try {
            if (messageReceiptHandle.getRenewRetryTimes() >= proxyConfig.getMaxRenewRetryTimes()) {
                log.warn("handle has exceed max renewRetryTimes. handle:{}", messageReceiptHandle);
                return CompletableFuture.completedFuture(null);
            }
            if (current - messageReceiptHandle.getTimestamp() < messageReceiptHandle.getExpectInvisibleTime()) {
                CompletableFuture<AckResult> future =
                    messagingProcessor.changeInvisibleTime(context, handle, messageReceiptHandle.getMessageId(),
                        messageReceiptHandle.getGroup(), messageReceiptHandle.getTopic(), proxyConfig.getRenewSliceTimeMillis());
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
                        resFuture.complete(messageReceiptHandle);
                    } else {
                        log.error("renew response is not ok. result:{}, handle:{}", ackResult, messageReceiptHandle, throwable);
                        resFuture.complete(null);
                    }
                });
            } else {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    messagingProcessor.getMetadataService().getSubscriptionGroupConfig(messageReceiptHandle.getGroup());
                if (subscriptionGroupConfig == null) {
                    log.error("group's subscriptionGroupConfig is null when renew. handle: {}", messageReceiptHandle);
                    return CompletableFuture.completedFuture(null);
                }
                RetryPolicy retryPolicy = subscriptionGroupConfig.getGroupRetryPolicy().getRetryPolicy();
                CompletableFuture<AckResult> future = messagingProcessor.changeInvisibleTime(context,
                    handle, messageReceiptHandle.getMessageId(), messageReceiptHandle.getGroup(),
                    messageReceiptHandle.getTopic(), retryPolicy.nextDelayDuration(messageReceiptHandle.getReconsumeTimes()));
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

    protected boolean clientIsOffline(ReceiptHandleGroupKey groupKey) {
        return this.messagingProcessor.findConsumerChannel(createContext("JudgeClientOnline"), groupKey.group, groupKey.clientId) == null;
    }

    public void addReceiptHandle(String clientID, String group, String msgID, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        this.addReceiptHandle(new ReceiptHandleGroupKey(clientID, group), msgID, receiptHandle, messageReceiptHandle);
    }

    protected void addReceiptHandle(ReceiptHandleGroupKey key, String msgID, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        if (key == null) {
            return;
        }
        ConcurrentHashMapUtils.computeIfAbsent(this.receiptHandleGroupMap, key,
            k -> new ReceiptHandleGroup()).put(msgID, receiptHandle, messageReceiptHandle);
    }

    public MessageReceiptHandle removeReceiptHandle(String clientID, String group, String msgID, String receiptHandle) {
        return this.removeReceiptHandle(new ReceiptHandleGroupKey(clientID, group), msgID, receiptHandle);
    }

    protected MessageReceiptHandle removeReceiptHandle(ReceiptHandleGroupKey key, String msgID, String receiptHandle) {
        if (key == null) {
            return null;
        }
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.get(key);
        if (handleGroup == null) {
            return null;
        }
        return handleGroup.remove(msgID, receiptHandle);
    }

    protected void clearGroup(ReceiptHandleGroupKey key) {
        if (key == null) {
            return;
        }
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        ProxyContext context = createContext("ClearGroup");
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.remove(key);
        if (handleGroup == null) {
            return;
        }
        handleGroup.scan((msgID, handle, v) -> {
            try {
                handleGroup.computeIfPresent(msgID, handle, messageReceiptHandle -> {
                    ReceiptHandle receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
                    messagingProcessor.changeInvisibleTime(
                        context,
                        receiptHandle,
                        messageReceiptHandle.getMessageId(),
                        messageReceiptHandle.getGroup(),
                        messageReceiptHandle.getTopic(),
                        proxyConfig.getInvisibleTimeMillisWhenClear()
                    );
                    return CompletableFuture.completedFuture(null);
                });
            } catch (Exception e) {
                log.error("error when clear handle for group. key:{}", key, e);
            }
        });
    }

    protected void clearAllHandle() {
        log.info("start clear all handle in receiptHandleProcessor");
        Set<ReceiptHandleGroupKey> keySet = receiptHandleGroupMap.keySet();
        for (ReceiptHandleGroupKey key : keySet) {
            clearGroup(key);
        }
        log.info("clear all handle in receiptHandleProcessor done");
    }

    public static class ReceiptHandleGroupKey {
        private final String clientId;
        private final String group;

        public ReceiptHandleGroupKey(String clientId, String group) {
            this.clientId = clientId;
            this.group = group;
        }

        public String getClientId() {
            return clientId;
        }

        public String getGroup() {
            return group;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReceiptHandleGroupKey key = (ReceiptHandleGroupKey) o;
            return Objects.equal(clientId, key.clientId) && Objects.equal(group, key.group);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(clientId, group);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("clientId", clientId)
                .add("group", group)
                .toString();
        }
    }
}
