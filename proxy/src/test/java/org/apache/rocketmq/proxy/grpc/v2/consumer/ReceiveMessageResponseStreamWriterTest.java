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

package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.processor.BatchChangeInvisibleTimeResult;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReceiveMessageResponseStreamWriterTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private ReceiveMessageResponseStreamWriter writer;
    private StreamObserver<ReceiveMessageResponse> streamObserver;

    @Before
    public void before() throws Throwable {
        super.before();
        this.streamObserver = mock(StreamObserver.class);
        this.writer = new ReceiveMessageResponseStreamWriter(this.messagingProcessor, this.streamObserver);
    }

    @Test
    public void testWriteMessage() {
        ArgumentCaptor<String> changeInvisibleTimeMsgIdCaptor = ArgumentCaptor.forClass(String.class);
        doReturn(CompletableFuture.completedFuture(mock(AckResult.class))).when(this.messagingProcessor)
            .changeInvisibleTime(any(), any(), changeInvisibleTimeMsgIdCaptor.capture(), anyString(), anyString(), anyLong(), any(), anyLong(), anyBoolean());

        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        AtomicInteger onNextCallNum = new AtomicInteger(0);
        doAnswer(mock -> {
            if (onNextCallNum.incrementAndGet() > 2) {
                throw new RuntimeException();
            }
            return null;
        }).when(streamObserver).onNext(responseArgumentCaptor.capture());

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        PopResult popResult = new PopResult(PopStatus.FOUND, messageExtList);
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
            .setFilterExpression(FilterExpression.newBuilder()
                .setType(FilterType.TAG)
                .setExpression("*")
                .build())
            .build();
        writer.writeAndComplete(
            ProxyContext.create(),
            receiveMessageRequest,
            popResult
        );

        verify(streamObserver, times(1)).onCompleted();
        verify(streamObserver, times(4)).onNext(any());
        verify(this.messagingProcessor, times(1))
            .changeInvisibleTime(any(), any(), anyString(), anyString(), anyString(), anyLong(), any(), anyLong(), eq(true));

        assertTrue(responseArgumentCaptor.getAllValues().get(0).hasStatus());
        assertEquals(Code.OK, responseArgumentCaptor.getAllValues().get(0).getStatus().getCode());
        assertTrue(responseArgumentCaptor.getAllValues().get(1).hasMessage());
        assertEquals(messageExtList.get(0).getMsgId(), responseArgumentCaptor.getAllValues().get(1).getMessage().getSystemProperties().getMessageId());

        assertEquals(messageExtList.get(1).getMsgId(), changeInvisibleTimeMsgIdCaptor.getValue());

        // case: fail to write response status at first step
        doThrow(new RuntimeException()).when(streamObserver).onNext(any());
        writer.writeAndComplete(
            ProxyContext.create(),
            receiveMessageRequest,
            popResult
        );
        verify(this.messagingProcessor, times(3))
            .changeInvisibleTime(any(), any(), anyString(), anyString(), anyString(), anyLong(), any(), anyLong(), eq(true));
    }

    @Test
    public void testPollingFull() {
        ArgumentCaptor<ReceiveMessageResponse> responseArgumentCaptor = ArgumentCaptor.forClass(ReceiveMessageResponse.class);
        doNothing().when(streamObserver).onNext(responseArgumentCaptor.capture());

        PopResult popResult = new PopResult(PopStatus.POLLING_FULL, new ArrayList<>());
        writer.writeAndComplete(
            ProxyContext.create(),
            ReceiveMessageRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
                .setFilterExpression(FilterExpression.newBuilder()
                    .setType(FilterType.TAG)
                    .setExpression("*")
                    .build())
                .build(),
            popResult
        );

        ReceiveMessageResponse response = responseArgumentCaptor.getAllValues().stream().filter(ReceiveMessageResponse::hasStatus)
            .findFirst().get();
        assertEquals(Code.TOO_MANY_REQUESTS, response.getStatus().getCode());
    }

    @Test
    public void testNackMessageWithSuspendTrue() {
        ArgumentCaptor<String> changeInvisibleTimeMsgIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> changeInvisibleTimeGroupCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> changeInvisibleTimeTopicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> changeInvisibleTimeInvisibleTimeCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> changeInvisibleTimeSuspendCaptor = ArgumentCaptor.forClass(Boolean.class);

        doReturn(CompletableFuture.completedFuture(mock(AckResult.class))).when(this.messagingProcessor)
            .changeInvisibleTime(any(), any(), changeInvisibleTimeMsgIdCaptor.capture(),
                changeInvisibleTimeGroupCaptor.capture(), changeInvisibleTimeTopicCaptor.capture(),
                changeInvisibleTimeInvisibleTimeCaptor.capture(), any(), anyLong(),
                changeInvisibleTimeSuspendCaptor.capture());

        MessageExt messageExt = createMessageExt(TOPIC, "tag");
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
            .build();

        // Simulate nack by calling processThrowableWhenWriteMessage using reflection
        // This is called when an exception occurs during message processing
        try {
            Method method = ReceiveMessageResponseStreamWriter.class.getDeclaredMethod(
                "processThrowableWhenWriteMessage",
                Throwable.class, ProxyContext.class, ReceiveMessageRequest.class, MessageExt.class);
            method.setAccessible(true);
            method.invoke(writer,
                new RuntimeException("Test exception"),
                ProxyContext.create(),
                receiveMessageRequest,
                messageExt);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Verify that changeInvisibleTime was called with suspend=true
        verify(this.messagingProcessor, times(1))
            .changeInvisibleTime(any(), any(), eq(messageExt.getMsgId()),
                eq(CONSUMER_GROUP), eq(TOPIC), eq(ReceiveMessageResponseStreamWriter.NACK_INVISIBLE_TIME),
                eq(null), eq(org.apache.rocketmq.proxy.processor.MessagingProcessor.DEFAULT_TIMEOUT_MILLS),
                eq(true));

        assertEquals(messageExt.getMsgId(), changeInvisibleTimeMsgIdCaptor.getValue());
        assertEquals(CONSUMER_GROUP, changeInvisibleTimeGroupCaptor.getValue());
        assertEquals(TOPIC, changeInvisibleTimeTopicCaptor.getValue());
        assertEquals(ReceiveMessageResponseStreamWriter.NACK_INVISIBLE_TIME,
            changeInvisibleTimeInvisibleTimeCaptor.getValue().longValue());
        assertTrue("Suspend should be true for nack", changeInvisibleTimeSuspendCaptor.getValue());
    }

    @Test
    public void testBatchNackWhenWriteStatusFailed() {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        ArgumentCaptor<List> handleMessageListCaptor = ArgumentCaptor.forClass(List.class);
        doReturn(CompletableFuture.completedFuture(new ArrayList<BatchChangeInvisibleTimeResult>()))
            .when(this.messagingProcessor).batchChangeInvisibleTime(
                any(), handleMessageListCaptor.capture(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
        doThrow(new RuntimeException("client cancelled")).when(streamObserver).onNext(any());

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        writer.writeAndComplete(
            ProxyContext.create(),
            createReceiveMessageRequest(),
            new PopResult(PopStatus.FOUND, messageExtList)
        );

        verify(this.messagingProcessor, times(1)).batchChangeInvisibleTime(
            any(), anyList(), eq(CONSUMER_GROUP), eq(TOPIC),
            eq(ReceiveMessageResponseStreamWriter.NACK_INVISIBLE_TIME),
            eq(org.apache.rocketmq.proxy.processor.MessagingProcessor.DEFAULT_TIMEOUT_MILLS),
            eq(true));
        verify(this.messagingProcessor, never()).changeInvisibleTime(
            any(), any(), anyString(), anyString(), anyString(), anyLong(), any(), anyLong(), anyBoolean());
        assertEquals(2, handleMessageListCaptor.getValue().size());
        assertEquals(messageExtList.get(0).getMsgId(),
            ((ReceiptHandleMessage) handleMessageListCaptor.getValue().get(0)).getMessageId());
        assertEquals(messageExtList.get(1).getMsgId(),
            ((ReceiptHandleMessage) handleMessageListCaptor.getValue().get(1)).getMessageId());
    }

    @Test
    public void testSingleNackWhenWriteStatusFailedUseSingleChangeInvisibleTime() {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        doReturn(CompletableFuture.completedFuture(mock(AckResult.class)))
            .when(this.messagingProcessor).changeInvisibleTime(
                any(), any(), anyString(), anyString(), anyString(), anyLong(), any(), anyLong(), anyBoolean());
        doThrow(new RuntimeException("client cancelled")).when(streamObserver).onNext(any());

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        writer.writeAndComplete(
            ProxyContext.create(),
            createReceiveMessageRequest(),
            new PopResult(PopStatus.FOUND, messageExtList)
        );

        verify(this.messagingProcessor, times(1)).changeInvisibleTime(
            any(), any(), eq(messageExtList.get(0).getMsgId()), eq(CONSUMER_GROUP), eq(TOPIC),
            eq(ReceiveMessageResponseStreamWriter.NACK_INVISIBLE_TIME), eq(null),
            eq(org.apache.rocketmq.proxy.processor.MessagingProcessor.DEFAULT_TIMEOUT_MILLS), eq(true));
        verify(this.messagingProcessor, never()).batchChangeInvisibleTime(
            any(), anyList(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());
    }

    @Test
    public void testBatchNackRemainingMessagesWhenClientCancelMidStream() {
        ConfigurationManager.getProxyConfig().setEnableBatchChangeInvisibleTime(true);
        ArgumentCaptor<List> handleMessageListCaptor = ArgumentCaptor.forClass(List.class);
        doReturn(CompletableFuture.completedFuture(new ArrayList<BatchChangeInvisibleTimeResult>()))
            .when(this.messagingProcessor).batchChangeInvisibleTime(
                any(), handleMessageListCaptor.capture(), anyString(), anyString(), anyLong(), anyLong(), anyBoolean());

        AtomicInteger onNextCallNum = new AtomicInteger(0);
        doAnswer(mock -> {
            if (onNextCallNum.incrementAndGet() == 3) {
                throw new RuntimeException("client cancelled");
            }
            return null;
        }).when(streamObserver).onNext(any());

        List<MessageExt> messageExtList = new ArrayList<>();
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        messageExtList.add(createMessageExt(TOPIC, "tag"));
        writer.writeAndComplete(
            ProxyContext.create(),
            createReceiveMessageRequest(),
            new PopResult(PopStatus.FOUND, messageExtList)
        );

        verify(this.messagingProcessor, times(1)).batchChangeInvisibleTime(
            any(), anyList(), eq(CONSUMER_GROUP), eq(TOPIC),
            eq(ReceiveMessageResponseStreamWriter.NACK_INVISIBLE_TIME),
            eq(org.apache.rocketmq.proxy.processor.MessagingProcessor.DEFAULT_TIMEOUT_MILLS),
            eq(true));
        verify(this.messagingProcessor, never()).changeInvisibleTime(
            any(), any(), anyString(), anyString(), anyString(), anyLong(), any(), anyLong(), anyBoolean());
        assertEquals(2, handleMessageListCaptor.getValue().size());
        assertEquals(messageExtList.get(1).getMsgId(),
            ((ReceiptHandleMessage) handleMessageListCaptor.getValue().get(0)).getMessageId());
        assertEquals(messageExtList.get(2).getMsgId(),
            ((ReceiptHandleMessage) handleMessageListCaptor.getValue().get(1)).getMessageId());
    }

    private static ReceiveMessageRequest createReceiveMessageRequest() {
        return ReceiveMessageRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .setMessageQueue(MessageQueue.newBuilder().setTopic(Resource.newBuilder().setName(TOPIC).build()).build())
            .setFilterExpression(FilterExpression.newBuilder()
                .setType(FilterType.TAG)
                .setExpression("*")
                .build())
            .build();
    }

    private static MessageExt createMessageExt(String topic, String tags) {
        String msgId = MessageClientIDSetter.createUniqID();

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setTags(tags);
        messageExt.setBody(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        messageExt.setMsgId(msgId);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, msgId);
        messageExt.setCommitLogOffset(RANDOM.nextInt(Integer.MAX_VALUE));
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_POP_CK,
            ExtraInfoUtil.buildExtraInfo(RANDOM.nextInt(Integer.MAX_VALUE), System.currentTimeMillis(), 3000,
                RANDOM.nextInt(Integer.MAX_VALUE), topic, "mockBroker", RANDOM.nextInt(Integer.MAX_VALUE), RANDOM.nextInt(Integer.MAX_VALUE)));
        return messageExt;
    }
}
