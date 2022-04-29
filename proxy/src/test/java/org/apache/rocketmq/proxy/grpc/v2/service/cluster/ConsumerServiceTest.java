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
package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerServiceTest extends BaseServiceTest {

    @Mock
    private ReadQueueSelector readQueueSelector;
    @Mock
    private StreamObserver<ReceiveMessageResponse> receiveMessageResponseStreamObserver;

    private ConsumerService consumerService;
    private DefaultReceiveMessageResultFilter receiveMessageResultFilter;

    @Override
    public void beforeEach() throws Throwable {
        consumerService = new ConsumerService(this.connectorManager, this.grpcClientManager);
        consumerService.start();

        receiveMessageResultFilter = new DefaultReceiveMessageResultFilter(producerClient, writeConsumerClient, grpcClientManager, topicRouteCache);
        consumerService.setReceiveMessageWriterBuilder((observer, hook) ->
            new DefaultReceiveMessageResponseStreamWriter(observer, hook, writeConsumerClient, topicRouteCache, receiveMessageResultFilter));
        consumerService.setReadQueueSelector(readQueueSelector);
    }

    @Test
    public void testReceiveMessage() throws Exception {
        SelectableMessageQueue selectableMessageQueue = new SelectableMessageQueue(
            new MessageQueue("namespace%topic", "brokerName", 0), "brokerAddr");
        when(readQueueSelector.select(any(), any(), any())).thenReturn(selectableMessageQueue);

        Settings clientSettings = Settings.newBuilder()
            .setBackoffPolicy(RetryPolicy.newBuilder().setMaxAttempts(16).build())
            .setSubscription(Subscription.newBuilder()
                .setFifo(false)
                .build())
            .build();
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        List<MessageExt> messageExtList = Lists.newArrayList(
            createMessageExt("msg1", "msg1"),
            createMessageExt("msg2", "msg2")
        );
        PopResult popResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(readConsumerClient.popMessage(any(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");
        when(writeConsumerClient.ackMessage(any(), anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(new AckResult()));

        Context ctx = Context.current().withDeadlineAfter(3, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor());
        AtomicReference<String> ackHandler = new AtomicReference<>();
        receiveMessageResultFilter.setAckNoMatchedMessageHook((ctx1, request, response, t) -> ackHandler.set(request.getExtraInfo()));
        consumerService.receiveMessage(ctx,
            ReceiveMessageRequest.newBuilder()
                .setMessageQueue(apache.rocketmq.v2.MessageQueue.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setResourceNamespace("namespace")
                        .setName("topic")
                        .build())
                    .build())
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("msg1")
                    .build())
                .build(),
            receiveMessageResponseStreamObserver
        );
        ArgumentCaptor<ReceiveMessageResponse> argument = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        verify(receiveMessageResponseStreamObserver, times(2)).onNext(argument.capture());
        verify(receiveMessageResponseStreamObserver, times(1)).onCompleted();

        ReceiveMessageResponse response = argument.getAllValues().get(0);
        assertTrue(response.hasStatus());
        assertEquals(Code.OK, response.getStatus().getCode());

        response = argument.getAllValues().get(1);
        assertTrue(response.hasMessage());
        assertEquals("msg1", response.getMessage().getSystemProperties().getMessageId());
        assertEquals(ReceiptHandle.create(messageExtList.get(1)).getReceiptHandle(), ackHandler.get());
    }

    @Test
    public void testToDLQInReceiveMessage() throws Exception {
        SelectableMessageQueue selectableMessageQueue = new SelectableMessageQueue(
            new MessageQueue("namespace%topic", "brokerName", 0), "brokerAddr");
        when(readQueueSelector.select(any(), any(), any())).thenReturn(selectableMessageQueue);

        Settings clientSettings = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .setBackoffPolicy(RetryPolicy.newBuilder().setMaxAttempts(0).build())
            .setSubscription(Subscription.newBuilder()
                .setFifo(false)
                .build())
            .build();
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        List<MessageExt> messageExtList = Lists.newArrayList(
            createMessageExt("msg1", "msg1"),
            createMessageExt("msg2", "msg2")
        );
        PopResult popResult = new PopResult(PopStatus.FOUND, messageExtList);
        when(readConsumerClient.popMessage(any(), anyString(), anyString(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(popResult));
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");
        ArgumentCaptor<ConsumerSendMsgBackRequestHeader> sendMsgBackRequestHeaderArgumentCaptor =
            ArgumentCaptor.forClass(ConsumerSendMsgBackRequestHeader.class);
        when(producerClient.sendMessageBackThenAckOrg(any(), anyString(), sendMsgBackRequestHeaderArgumentCaptor.capture(), any()))
            .thenReturn(CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "")));

        Context ctx = Context.current().withDeadlineAfter(3, TimeUnit.SECONDS, Executors.newSingleThreadScheduledExecutor());
        consumerService.receiveMessage(ctx,
            ReceiveMessageRequest.newBuilder()
                .setMessageQueue(apache.rocketmq.v2.MessageQueue.newBuilder()
                    .setTopic(Resource.newBuilder()
                        .setResourceNamespace("namespace")
                        .setName("topic")
                        .build())
                    .build())
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("msg1")
                    .build())
                .build(),
            receiveMessageResponseStreamObserver
        );
        ArgumentCaptor<ReceiveMessageResponse> argument = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        verify(receiveMessageResponseStreamObserver, times(1)).onNext(argument.capture());
        verify(receiveMessageResponseStreamObserver, times(1)).onCompleted();

        ReceiveMessageResponse response = argument.getValue();
        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(2, sendMsgBackRequestHeaderArgumentCaptor.getAllValues().size());
        Set<String> toDLQMsgId = sendMsgBackRequestHeaderArgumentCaptor.getAllValues().stream()
            .map(ConsumerSendMsgBackRequestHeader::getOriginMsgId).collect(Collectors.toSet());
        assertTrue(toDLQMsgId.contains("msg1"));
        assertTrue(toDLQMsgId.contains("msg2"));
    }

    @Test
    public void testAckMessage() throws Exception {
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        when(writeConsumerClient.ackMessage(any(), anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(ackResult));

        AckMessageResponse response = consumerService.ackMessage(Context.current(), AckMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .addEntries(AckMessageEntry.newBuilder()
                .setMessageId("msgId")
                .setReceiptHandle(createReceiptHandle().encode()))
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
    }

    @Test
    public void testNackMessageToDLQ() throws Exception {
        ReceiptHandle receiptHandle = createReceiptHandle();
        ArgumentCaptor<ConsumerSendMsgBackRequestHeader> headerArgumentCaptor = ArgumentCaptor.forClass(ConsumerSendMsgBackRequestHeader.class);
        when(producerClient.sendMessageBackThenAckOrg(any(), anyString(), headerArgumentCaptor.capture(), any()))
            .thenReturn(CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "")));
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");

        Settings clientSettings = createClientSettings(3);
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        NackMessageResponse response = consumerService.nackMessage(Context.current(), NackMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setReceiptHandle(receiptHandle.encode())
            .setDeliveryAttempt(3)
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(receiptHandle.getCommitLogOffset(), headerArgumentCaptor.getValue().getOffset().longValue());
    }

    @Test
    public void testNackMessage() throws Exception {
        ReceiptHandle receiptHandle = createReceiptHandle();
        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> headerArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        when(writeConsumerClient.changeInvisibleTimeAsync(any(), anyString(), anyString(), anyString(), headerArgumentCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(ackResult));
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");

        Settings clientSettings = createClientSettings(3);
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        NackMessageResponse response = consumerService.nackMessage(Context.current(), NackMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setReceiptHandle(receiptHandle.encode())
            .setDeliveryAttempt(1)
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(receiptHandle.getOffset(), headerArgumentCaptor.getValue().getOffset().longValue());
        assertEquals(receiptHandle.encode(), headerArgumentCaptor.getValue().getExtraInfo());
    }

    @Test
    public void testChangeInvisibleDuration() throws Exception {
        Duration newDuration = Duration.newBuilder()
            .setSeconds(3).build();
        ReceiptHandle receiptHandle = createReceiptHandle();
        ArgumentCaptor<ChangeInvisibleTimeRequestHeader> headerArgumentCaptor = ArgumentCaptor.forClass(ChangeInvisibleTimeRequestHeader.class);
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.OK);
        ackResult.setExtraInfo(receiptHandle.encode());
        when(writeConsumerClient.changeInvisibleTimeAsync(any(), anyString(), anyString(), anyString(), headerArgumentCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(ackResult));
        when(topicRouteCache.getBrokerAddr(anyString())).thenReturn("brokerAddr");

        Settings clientSettings = createClientSettings(3);
        when(grpcClientManager.getClientSettings(any(Context.class))).thenReturn(clientSettings);

        ChangeInvisibleDurationResponse response = consumerService.changeInvisibleDuration(Context.current(), ChangeInvisibleDurationRequest.newBuilder()
            .setTopic(Resource.newBuilder()
                .setName("topic")
                .build())
            .setGroup(Resource.newBuilder()
                .setName("group")
                .build())
            .setReceiptHandle(receiptHandle.encode())
            .setInvisibleDuration(newDuration)
            .build())
            .get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(receiptHandle.getOffset(), headerArgumentCaptor.getValue().getOffset().longValue());
        assertEquals(receiptHandle.encode(), headerArgumentCaptor.getValue().getExtraInfo());
        assertEquals(Durations.toMillis(newDuration), headerArgumentCaptor.getValue().getInvisibleTime().longValue());
    }

    private Settings createClientSettings(int maxDeliveryAttempts) {
        return Settings.newBuilder()
            .setBackoffPolicy(RetryPolicy.newBuilder()
                .setMaxAttempts(maxDeliveryAttempts)
                .build())
            .setSubscription(Subscription.newBuilder()
                .build())
            .build();
    }
}