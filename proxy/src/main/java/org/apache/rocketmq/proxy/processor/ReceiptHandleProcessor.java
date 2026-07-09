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

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.state.StateEventListener;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.RenewEvent;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.receipt.DefaultReceiptHandleManager;

public class ReceiptHandleProcessor extends AbstractProcessor {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final String RENEW_BATCH_KEY_SEPARATOR = "\u0001";

    protected DefaultReceiptHandleManager receiptHandleManager;

    public ReceiptHandleProcessor(MessagingProcessor messagingProcessor, ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
        StateEventListener<RenewEvent> eventListener = event -> {
            ProxyContext context = createContext(event.getEventType().name())
                .setChannel(event.getKey().getChannel());
            List<MessageReceiptHandle> messageReceiptHandleList = event.getMessageReceiptHandleList();
            if (messageReceiptHandleList == null || messageReceiptHandleList.isEmpty()) {
                return;
            }
            if (messageReceiptHandleList.size() > 1) {
                batchChangeInvisibleTime(context, event);
                return;
            }
            MessageReceiptHandle messageReceiptHandle = messageReceiptHandleList.get(0);
            ReceiptHandle handle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
            messagingProcessor
                .changeInvisibleTime(context, handle, messageReceiptHandle.getMessageId(),
                    messageReceiptHandle.getGroup(), messageReceiptHandle.getTopic(),
                    event.getRenewTime(), messageReceiptHandle.getLiteTopic())
                .whenComplete((v, t) -> {
                    if (t != null) {
                        event.getFuture().completeExceptionally(t);
                        return;
                    }
                    event.getFuture().complete(v);
                });
        };
        this.receiptHandleManager = new DefaultReceiptHandleManager(serviceManager.getMetadataService(), serviceManager.getConsumerManager(), eventListener);
        this.appendStartAndShutdown(receiptHandleManager);
    }

    protected void batchChangeInvisibleTime(ProxyContext context, RenewEvent event) {
        List<MessageReceiptHandle> messageReceiptHandleList = event.getMessageReceiptHandleList();
        List<Long> renewTimeList = event.getRenewTimeList();
        List<CompletableFuture<AckResult>> futureList = event.getFutureList();
        Map<String, List<Integer>> indexesByGroupAndTopic = new HashMap<>();
        for (int i = 0; i < messageReceiptHandleList.size(); i++) {
            MessageReceiptHandle messageReceiptHandle = messageReceiptHandleList.get(i);
            String key = messageReceiptHandle.getGroup() + RENEW_BATCH_KEY_SEPARATOR + messageReceiptHandle.getTopic();
            indexesByGroupAndTopic.computeIfAbsent(key, ignored -> new ArrayList<>()).add(i);
        }

        for (List<Integer> indexes : indexesByGroupAndTopic.values()) {
            MessageReceiptHandle firstHandle = messageReceiptHandleList.get(indexes.get(0));
            List<ReceiptHandleMessage> handleMessageList = new ArrayList<>(indexes.size());
            for (Integer index : indexes) {
                MessageReceiptHandle messageReceiptHandle = messageReceiptHandleList.get(index);
                handleMessageList.add(new ReceiptHandleMessage(
                    ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr()),
                    messageReceiptHandle.getMessageId(),
                    messageReceiptHandle.getLiteTopic(),
                    renewTimeList.get(index)));
            }
            messagingProcessor.batchChangeInvisibleTime(
                    context,
                    handleMessageList,
                    firstHandle.getGroup(),
                    firstHandle.getTopic(),
                    renewTimeList.get(indexes.get(0)),
                    MessagingProcessor.DEFAULT_TIMEOUT_MILLS,
                    false)
                .whenComplete((results, throwable) -> {
                    if (throwable != null) {
                        indexes.forEach(index -> futureList.get(index).completeExceptionally(throwable));
                        return;
                    }
                    for (int i = 0; i < indexes.size(); i++) {
                        CompletableFuture<AckResult> future = futureList.get(indexes.get(i));
                        if (results == null || i >= results.size()) {
                            future.completeExceptionally(new IllegalStateException("batch change invisible time result missing"));
                            continue;
                        }
                        BatchChangeInvisibleTimeResult result = results.get(i);
                        if (result.getProxyException() != null) {
                            future.completeExceptionally(result.getProxyException());
                        } else {
                            future.complete(result.getAckResult());
                        }
                    }
                });
        }
    }

    protected ProxyContext createContext(String actionName) {
        return ProxyContext.createForInner(this.getClass().getSimpleName() + actionName);
    }

    public void addReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID, MessageReceiptHandle messageReceiptHandle) {
        receiptHandleManager.addReceiptHandle(ctx, channel, group, msgID, messageReceiptHandle);
    }

    public MessageReceiptHandle removeReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID, String receiptHandle) {
        return receiptHandleManager.removeReceiptHandle(ctx, channel, group, msgID, receiptHandle);
    }

    public int getUnackedMessageCount(ProxyContext ctx, Channel channel, String group) {
        return receiptHandleManager.getUnackedMessageCount(ctx, channel, group);
    }

}
