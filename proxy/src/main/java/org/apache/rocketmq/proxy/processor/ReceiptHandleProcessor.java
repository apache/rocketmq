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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ReceiptHandleGroup;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiptHandleProcessor extends AbstractStartAndShutdown {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected final ConcurrentMap<String, ReceiptHandleGroup> receiptHandleGroupMap;
    protected final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RenewalScheduledThread_"));
    protected final ExecutorService renewalWorkerService = ThreadPoolMonitor.createAndMonitor(
        2, 4, 0, TimeUnit.MILLISECONDS,
        "RenewalWorkerThread_", 10000
    );
    protected final MessagingProcessor messagingProcessor;

    public ReceiptHandleProcessor(MessagingProcessor messagingProcessor) {
        this.messagingProcessor = messagingProcessor;
        this.messagingProcessor.registerConsumerListener(new ConsumerIdsChangeListener() {
            @Override
            public void handle(ConsumerGroupEvent event, String group, Object... args) {
                if (ConsumerGroupEvent.CLIENT_UNREGISTER.equals(event)) {
                    if (args == null || args.length < 1) {
                        return;
                    }
                    if (args[0] instanceof ClientChannelInfo) {
                        ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                        clearGroup(buildKey(clientChannelInfo.getClientId(), group));
                    }
                }
            }

            @Override
            public void shutdown() {

            }
        });
        this.receiptHandleGroupMap = new ConcurrentHashMap<>();
        this.init();
    }

    protected void init() {
        this.appendStartAndShutdown(new StartAndShutdown() {
            @Override
            public void start() throws Exception {
                log.info("scan for renewal start.");
                scheduledExecutorService.scheduleAtFixedRate(() -> scheduleRenewTask(), 0,
                    ConfigurationManager.getProxyConfig().getRenewSchedulePeriodMillis(), TimeUnit.MILLISECONDS);
                log.info("renewal queue has started");
            }

            @Override
            public void shutdown() throws Exception {
                scheduledExecutorService.shutdown();
            }
        });
    }

    protected ProxyContext createContext(String actionName) {
        return ProxyContext.createForInner(this.getClass().getSimpleName() + actionName);
    }

    protected void scheduleRenewTask() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        for (Map.Entry<String, ReceiptHandleGroup> entry : receiptHandleGroupMap.entrySet()) {
            String key = entry.getKey();
            Pair<String, String> clientIdAndGroup = parseKey(key);
            if (clientIdAndGroup == null) {
                log.warn("client id and group is empty. key:{}, receiptHandleGroup:{}", key, entry.getValue());
                clearGroup(key);
                continue;
            }
            if (clientIsOffline(clientIdAndGroup.getLeft(), clientIdAndGroup.getRight())) {
                clearGroup(key);
                continue;
            }

            ReceiptHandleGroup group = entry.getValue();
            group.scan((msgID, handleStr, v) -> {
                ReceiptHandle handle = ReceiptHandle.decode(v.getReceiptHandle());
                long now = System.currentTimeMillis();
                if (handle.getNextVisibleTime() - now > proxyConfig.getRenewAheadTimeMillis()) {
                    return;
                }
                SubscriptionGroupConfig subscriptionGroupConfig =
                    messagingProcessor.getMetadataService().getSubscriptionGroupConfig(v.getGroup());
                if (subscriptionGroupConfig == null) {
                    log.error("Group's subscriptionGroupConfig is null, group = {}", v.getGroup());
                    return;
                }
                RetryPolicy retryPolicy = subscriptionGroupConfig.getGroupRetryPolicy().getRetryPolicy();
                renewalWorkerService.submit(() -> renewMessage(key, msgID, v, handle, retryPolicy));
            });
        }

        log.info("scan for renewal done.");
    }

    protected void renewMessage(String key, String msgID, MessageReceiptHandle messageReceiptHandle,
        ReceiptHandle handle, RetryPolicy retryPolicy) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        ProxyContext context = createContext("RenewMessage");
        long current = System.currentTimeMillis();
        if (current - messageReceiptHandle.getTimestamp() < messageReceiptHandle.getExpectInvisibleTime()) {
            CompletableFuture<AckResult> future =
                messagingProcessor.changeInvisibleTime(context, handle, messageReceiptHandle.getMessageId(),
                    messageReceiptHandle.getGroup(), messageReceiptHandle.getTopic(), proxyConfig.getRenewSliceTimeMillis());
            future.thenAccept(ackResult -> {
                if (AckStatus.OK.equals(ackResult.getStatus())) {
                    messageReceiptHandle.update(ackResult.getExtraInfo());
                    addReceiptHandle(key, msgID, messageReceiptHandle.getOriginalReceiptHandle(), messageReceiptHandle);
                }
            });
        } else {
            CompletableFuture<AckResult> future = messagingProcessor.changeInvisibleTime(context,
                handle, messageReceiptHandle.getMessageId(), messageReceiptHandle.getGroup(),
                messageReceiptHandle.getTopic(), retryPolicy.nextDelayDuration(messageReceiptHandle.getReconsumeTimes()));
            future.thenAccept(ackResult -> {
                if (AckStatus.OK.equals(ackResult.getStatus())) {
                    removeReceiptHandle(key, msgID, messageReceiptHandle.getOriginalReceiptHandle());
                }
            });
        }
    }

    protected String buildKey(String clientID, String group) {
        return clientID + "%" + group;
    }

    protected Pair<String, String> parseKey(String key) {
        String[] strs = key.split("%");
        if (strs.length < 2) {
            return null;
        }
        return Pair.of(strs[0], strs[1]);
    }

    protected boolean clientIsOffline(String clientID, String group) {
        return this.messagingProcessor.findConsumerChannel(createContext("JudgeClientOnline"), group, clientID) == null;
    }

    public void addReceiptHandle(String clientID, String group, String msgID, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        this.addReceiptHandle(buildKey(clientID, group), msgID, receiptHandle, messageReceiptHandle);
    }

    protected void addReceiptHandle(String key, String msgID, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        if (key == null) {
            return;
        }
        receiptHandleGroupMap.computeIfAbsent(key,
            k -> new ReceiptHandleGroup()).put(msgID, receiptHandle, messageReceiptHandle);
    }

    public MessageReceiptHandle removeReceiptHandle(String clientID, String group, String msgID, String receiptHandle) {
        return this.removeReceiptHandle(buildKey(clientID, group), msgID, receiptHandle);
    }

    protected MessageReceiptHandle removeReceiptHandle(String key, String msgID, String receiptHandle) {
        if (key == null) {
            return null;
        }
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        receiptHandleGroupMap.computeIfPresent(key, (k, v) -> {
            res.set(v.remove(msgID, receiptHandle));
            if (v.isEmpty()) {
                return null;
            }
            return v;
        });
        return res.get();
    }

    public MessageReceiptHandle removeOneReceiptHandle(String clientID, String group, String msgID) {
        return removeOneReceiptHandle(buildKey(clientID, group), msgID);
    }

    protected MessageReceiptHandle removeOneReceiptHandle(String key, String msgID) {
        if (key == null) {
            return null;
        }
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        receiptHandleGroupMap.computeIfPresent(key, (k, v) -> {
            res.set(v.removeOne(msgID));
            if (v.isEmpty()) {
                return null;
            }
            return v;
        });
        return res.get();
    }

    protected void clearGroup(String key) {
        if (key == null) {
            return;
        }
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        ProxyContext context = createContext("ClearGroup");
        ReceiptHandleGroup handleGroup = receiptHandleGroupMap.remove(key);
        if (handleGroup == null) {
            return;
        }
        handleGroup.scan((msgID, handle, messageReceiptHandle) -> {
            ReceiptHandle receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandle());
            messagingProcessor.changeInvisibleTime(
                context,
                receiptHandle,
                messageReceiptHandle.getMessageId(),
                messageReceiptHandle.getGroup(),
                messageReceiptHandle.getTopic(),
                proxyConfig.getInvisibleTimeMillisWhenClear()
            );
        });
    }
}
