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

    protected void scheduleRenewTask() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        for (Map.Entry<String, ReceiptHandleGroup> entry : receiptHandleGroupMap.entrySet()) {
            String key = entry.getKey();
            ReceiptHandleGroup group = entry.getValue();
            group.immutableMapView().forEach((k, v) -> {
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
                renewalWorkerService.submit(() -> renewMessage(key, v, handle, retryPolicy));
            });
        }

        log.info("scan for renewal done.");
    }

    protected void renewMessage(String key, MessageReceiptHandle messageReceiptHandle,
        ReceiptHandle handle, RetryPolicy retryPolicy) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        long current = System.currentTimeMillis();
        if (current - messageReceiptHandle.getTimestamp() < messageReceiptHandle.getExpectInvisibleTime()) {
            CompletableFuture<AckResult> future =
                messagingProcessor.changeInvisibleTime(ProxyContext.create(), handle, messageReceiptHandle.getMessageId(),
                    messageReceiptHandle.getGroup(), messageReceiptHandle.getTopic(), proxyConfig.getRenewSliceTimeMillis());
            future.thenAccept(ackResult -> {
                if (AckStatus.OK.equals(ackResult.getStatus())) {
                    messageReceiptHandle.update(ackResult.getExtraInfo());
                    addReceiptHandle(key, messageReceiptHandle.getOriginalReceiptHandle(), messageReceiptHandle);
                }
            });
        } else {
            CompletableFuture<AckResult> future = messagingProcessor.changeInvisibleTime(ProxyContext.create(),
                handle, messageReceiptHandle.getMessageId(), messageReceiptHandle.getGroup(),
                messageReceiptHandle.getTopic(), retryPolicy.nextDelayDuration(messageReceiptHandle.getReconsumeTimes()));
            future.thenAccept(ackResult -> {
                if (AckStatus.OK.equals(ackResult.getStatus())) {
                    removeReceiptHandle(key, messageReceiptHandle.getOriginalReceiptHandle());
                }
            });
        }
    }

    protected String buildKey(String clientID, String group) {
        return clientID + "%" + group;
    }

    public void addReceiptHandle(String clientID, String group, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        this.addReceiptHandle(buildKey(clientID, group), receiptHandle, messageReceiptHandle);
    }

    protected void addReceiptHandle(String key, String receiptHandle,
        MessageReceiptHandle messageReceiptHandle) {
        if (key == null) {
            return;
        }
        receiptHandleGroupMap.computeIfAbsent(key,
            k -> new ReceiptHandleGroup()).put(receiptHandle, messageReceiptHandle);
    }

    public void removeReceiptHandle(String clientID, String group, String receiptHandle) {
        this.removeReceiptHandle(buildKey(clientID, group), receiptHandle);
    }

    protected void removeReceiptHandle(String key, String receiptHandle) {
        if (key == null) {
            return;
        }
        receiptHandleGroupMap.computeIfPresent(key, (k, v) -> {
            v.remove(receiptHandle);
            return v;
        });
    }

    public void clearGroup(String key) {
        if (key == null) {
            return;
        }
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        receiptHandleGroupMap.computeIfPresent(key, (k, v) -> {
                Map<String, MessageReceiptHandle> all = v.immutableMapView();
                all.forEach((key0, value0) -> {
                    ReceiptHandle receiptHandle = ReceiptHandle.decode(value0.getReceiptHandle());
                    messagingProcessor.changeInvisibleTime(
                        ProxyContext.create(),
                        receiptHandle,
                        value0.getMessageId(),
                        value0.getGroup(),
                        value0.getTopic(),
                        proxyConfig.getInvisibleTimeMillisWhenClear()
                    );
                });
                return null;
            }
        );
    }
}
