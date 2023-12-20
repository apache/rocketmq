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
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;

public class ClusterMessageService implements MessageService {
    protected final TopicRouteService topicRouteService;
    protected final MQClientAPIFactory mqClientAPIFactory;

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
            this.resolveBrokerAddrInReceiptHandle(ctx, handle),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName,
        EndTransactionRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            this.mqClientAPIFactory.getClient().endTransactionOneway(
                this.resolveBrokerAddr(ctx, brokerName),
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
        requestHeader.setBornTime(System.currentTimeMillis());
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
            this.resolveBrokerAddrInReceiptHandle(ctx, handle),
            handle.getBrokerName(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().ackMessageAsync(
            this.resolveBrokerAddrInReceiptHandle(ctx, handle),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<AckResult> batchAckMessage(ProxyContext ctx, List<ReceiptHandleMessage> handleList, String consumerGroup,
        String topic, long timeoutMillis) {
        List<String> extraInfoList = handleList.stream().map(message -> message.getReceiptHandle().getReceiptHandle()).collect(Collectors.toList());
        return this.mqClientAPIFactory.getClient().batchAckMessageAsync(
            this.resolveBrokerAddrInReceiptHandle(ctx, handleList.get(0).getReceiptHandle()),
            topic,
            consumerGroup,
            extraInfoList,
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

    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        try {
            String brokerAddress = topicRouteService.getBrokerAddr(ctx, brokerName);
            return mqClientAPIFactory.getClient().invoke(brokerAddress, request, timeoutMillis);
        } catch (Throwable t) {
            return FutureUtils.completeExceptionally(t);
        }
    }

    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        try {
            String brokerAddress = topicRouteService.getBrokerAddr(ctx, brokerName);
            return mqClientAPIFactory.getClient().invokeOneway(brokerAddress, request, timeoutMillis);
        } catch (Throwable t) {
            return FutureUtils.completeExceptionally(t);
        }
    }

    protected String resolveBrokerAddrInReceiptHandle(ProxyContext ctx, ReceiptHandle handle) {
        try {
            return this.topicRouteService.getBrokerAddr(ctx, handle.getBrokerName());
        } catch (Throwable t) {
            throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "cannot find broker " + handle.getBrokerName(), t);
        }
    }

    protected String resolveBrokerAddr(ProxyContext ctx, String brokerName) {
        try {
            return this.topicRouteService.getBrokerAddr(ctx, brokerName);
        } catch (Throwable t) {
            throw new ProxyException(ProxyExceptionCode.INVALID_BROKER_NAME, "cannot find broker " + brokerName, t);
        }
    }
}
