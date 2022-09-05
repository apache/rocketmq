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
package org.apache.rocketmq.proxy.service.message;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ClusterMessageService implements MessageService {
    private final TopicRouteService topicRouteService;
    private final MQClientAPIFactory mqClientAPIFactory;

    public ClusterMessageService(TopicRouteService topicRouteService, MQClientAPIFactory mqClientAPIFactory) {
        this.topicRouteService = topicRouteService;
        this.mqClientAPIFactory = mqClientAPIFactory;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        List<Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
        CompletableFuture<List<SendResult>> future;
        if (msgList.size() == 1) {
            future = this.mqClientAPIFactory.getClient().sendMessageAsync(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(), msgList.get(0), requestHeader, timeoutMillis)
                .thenApply(Lists::newArrayList);
        } else {
            future = this.mqClientAPIFactory.getClient().sendMessageAsync(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(), msgList, requestHeader, timeoutMillis)
                .thenApply(Lists::newArrayList);
        }
        return future;
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessageBack(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().sendMessageBackAsync(
            this.resolveBrokerAddrInReceiptHandle(handle),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName, EndTransactionRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            this.mqClientAPIFactory.getClient().endTransactionOneway(
                this.resolveBrokerAddr(brokerName),
                requestHeader,
                "end transaction from proxy",
                timeoutMillis
            );
            future.complete(null);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().popMessageAsync(
            messageQueue.getBrokerAddr(),
            messageQueue.getBrokerName(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().changeInvisibleTimeAsync(
            this.resolveBrokerAddrInReceiptHandle(handle),
            handle.getBrokerName(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().ackMessageAsync(
            this.resolveBrokerAddrInReceiptHandle(handle),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().pullMessageAsync(
            messageQueue.getBrokerAddr(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().queryConsumerOffsetWithFuture(
            messageQueue.getBrokerAddr(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().updateConsumerOffsetOneWay(
            messageQueue.getBrokerAddr(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        LockBatchRequestBody requestBody, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().lockBatchMQWithFuture(
            messageQueue.getBrokerAddr(),
            requestBody,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().unlockBatchMQOneway(
            messageQueue.getBrokerAddr(),
            requestBody,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().getMaxOffset(
            messageQueue.getBrokerAddr(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().getMinOffset(
            messageQueue.getBrokerAddr(),
            requestHeader,
            timeoutMillis
        );
    }

    protected String resolveBrokerAddrInReceiptHandle(ReceiptHandle handle) {
        try {
            return this.topicRouteService.getBrokerAddr(handle.getBrokerName());
        } catch (Throwable t) {
            throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "cannot find broker " + handle.getBrokerName(), t);
        }
    }

    protected String resolveBrokerAddr(String brokerName) {
        try {
            return this.topicRouteService.getBrokerAddr(brokerName);
        } catch (Throwable t) {
            throw new ProxyException(ProxyExceptionCode.INVALID_BROKER_NAME, "cannot find broker " + brokerName, t);
        }
    }
}
