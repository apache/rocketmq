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
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ClusterMessageService implements MessageService {

    private final TopicRouteService topicRouteService;
    private final MQClientAPIFactory mqClientAPIFactory;

    public ClusterMessageService(TopicRouteService topicRouteService, MQClientAPIFactory mqClientAPIFactory) {
        this.topicRouteService = topicRouteService;
        this.mqClientAPIFactory = mqClientAPIFactory;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, SelectableMessageQueue messageQueue,
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
            this.resolveBrokerAddr(handle),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public void endTransactionOneway(ProxyContext ctx, TransactionId transactionId,
        EndTransactionRequestHeader requestHeader,
        long timeoutMillis) throws MQBrokerException, RemotingException, InterruptedException {
        this.mqClientAPIFactory.getClient().endTransactionOneway(
            this.resolveBrokerAddr(transactionId.getBrokerName()),
            requestHeader,
            "end transaction from proxy",
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, SelectableMessageQueue messageQueue,
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
            this.resolveBrokerAddr(handle),
            handle.getBrokerName(),
            requestHeader,
            timeoutMillis
        );
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        return this.mqClientAPIFactory.getClient().ackMessageAsync(
            this.resolveBrokerAddr(handle),
            requestHeader,
            timeoutMillis
        );
    }

    protected String resolveBrokerAddr(ReceiptHandle handle) {
        return resolveBrokerAddr(handle.getBrokerName());
    }

    protected String resolveBrokerAddr(String brokerName) {
        try {
            return this.topicRouteService.getBrokerAddr(brokerName);
        } catch (Throwable t) {
            throw new ProxyException(ProxyExceptionCode.INVALID_BROKER_NAME, "broker " + brokerName + " cannot find", t);
        }
    }
}
