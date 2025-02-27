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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.state.StateEventListener;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.RenewEvent;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.receipt.DefaultReceiptHandleManager;

public class ReceiptHandleProcessor extends AbstractProcessor {
    protected final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected DefaultReceiptHandleManager receiptHandleManager;

    public ReceiptHandleProcessor(MessagingProcessor messagingProcessor, ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
        StateEventListener<RenewEvent> eventListener = event -> {
            ProxyContext context = createContext(event.getEventType().name())
                .setChannel(event.getKey().getChannel());
            MessageReceiptHandle messageReceiptHandle = event.getMessageReceiptHandle();
            ReceiptHandle handle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
            messagingProcessor.changeInvisibleTime(context, handle, messageReceiptHandle.getMessageId(),
                    messageReceiptHandle.getGroup(), messageReceiptHandle.getTopic(), event.getRenewTime())
                .whenComplete((v, t) -> {
                    if (t != null) {
                        event.getFuture().completeExceptionally(t);
                        return;
                    }
                    event.getFuture().complete(v);
                });
        };
        this.receiptHandleManager = new DefaultReceiptHandleManager(serviceManager.getMetadataService(), serviceManager.getConsumerManager(), eventListener);
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

}
