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

import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Resource;
import com.google.protobuf.util.Durations;
import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChangeInvisibleDurationActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private ChangeInvisibleDurationActivity changeInvisibleDurationActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.changeInvisibleDurationActivity = new ChangeInvisibleDurationActivity(messagingProcessor,
            grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testChangeInvisibleDurationActivity() throws Throwable {
        String newHandle = "newHandle";
        ArgumentCaptor<Long> invisibleTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        AckResult ackResult = new AckResult();
        ackResult.setExtraInfo(newHandle);
        ackResult.setStatus(AckStatus.OK);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            invisibleTimeArgumentCaptor.capture(),
            anyString(),  // request.getLiteTopic()
            anyLong(),    // MessagingProcessor.DEFAULT_TIMEOUT_MILLS
            anyBoolean()  // request.getSuspend()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));

        ChangeInvisibleDurationResponse response = this.changeInvisibleDurationActivity.changeInvisibleDuration(
            createContext(),
            ChangeInvisibleDurationRequest.newBuilder()
                .setInvisibleDuration(Durations.fromSeconds(3))
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageId("msgId")
                .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(TimeUnit.SECONDS.toMillis(3), invisibleTimeArgumentCaptor.getValue().longValue());
        assertEquals(newHandle, response.getReceiptHandle());
    }

    @Test
    public void testChangeInvisibleDurationActivityWhenHasMappingHandle() throws Throwable {
        String newHandle = "newHandle";
        ArgumentCaptor<Long> invisibleTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        AckResult ackResult = new AckResult();
        ackResult.setExtraInfo(newHandle);
        ackResult.setStatus(AckStatus.OK);
        String savedHandleStr = buildReceiptHandle("topic", System.currentTimeMillis(),3000);
        ArgumentCaptor<ReceiptHandle> receiptHandleCaptor = ArgumentCaptor.forClass(ReceiptHandle.class);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            receiptHandleCaptor.capture(),
            anyString(),
            anyString(),
            anyString(),
            invisibleTimeArgumentCaptor.capture(),
            anyString(),  // request.getLiteTopic()
            anyLong(),    // MessagingProcessor.DEFAULT_TIMEOUT_MILLS
            anyBoolean()  // request.getSuspend()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));
        when(messagingProcessor.removeReceiptHandle(any(), any(), anyString(), anyString(), anyString()))
            .thenReturn(new MessageReceiptHandle("group", "topic", 0, savedHandleStr, "msgId", 0, 0));

        ChangeInvisibleDurationResponse response = this.changeInvisibleDurationActivity.changeInvisibleDuration(
            createContext(),
            ChangeInvisibleDurationRequest.newBuilder()
                .setInvisibleDuration(Durations.fromSeconds(3))
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageId("msgId")
                .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(TimeUnit.SECONDS.toMillis(3), invisibleTimeArgumentCaptor.getValue().longValue());
        assertEquals(savedHandleStr, receiptHandleCaptor.getValue().getReceiptHandle());
        assertEquals(newHandle, response.getReceiptHandle());
    }


    @Test
    public void testChangeInvisibleDurationActivityFailed() throws Throwable {
        ArgumentCaptor<Long> invisibleTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.NO_EXIST);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            invisibleTimeArgumentCaptor.capture(),
            anyString(),  // request.getLiteTopic()
            anyLong(),    // MessagingProcessor.DEFAULT_TIMEOUT_MILLS
            anyBoolean()  // request.getSuspend()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));
        ChangeInvisibleDurationResponse response = this.changeInvisibleDurationActivity.changeInvisibleDuration(
            createContext(),
            ChangeInvisibleDurationRequest.newBuilder()
                .setInvisibleDuration(Durations.fromSeconds(3))
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageId("msgId")
                .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                .build()
        ).get();

        assertEquals(Code.INTERNAL_SERVER_ERROR, response.getStatus().getCode());
        assertEquals(TimeUnit.SECONDS.toMillis(3), invisibleTimeArgumentCaptor.getValue().longValue());
    }

    @Test
    public void testChangeInvisibleDurationReRegistersNewHandleWhenOldHandleIsManaged() throws Throwable {
        String oldHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000);
        String newHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 5000);
        String msgId = "msgId";
        grpcChannelManager.createChannel(createContext(), CLIENT_ID);
        MessageReceiptHandle messageReceiptHandle =
            new MessageReceiptHandle(CONSUMER_GROUP, TOPIC, 0, oldHandle, msgId, 1, 2);
        AckResult ackResult = new AckResult();
        ackResult.setExtraInfo(newHandle);
        ackResult.setStatus(AckStatus.OK);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyBoolean()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));
        when(messagingProcessor.removeReceiptHandle(any(), any(), anyString(), anyString(), anyString()))
            .thenReturn(messageReceiptHandle);

        ChangeInvisibleDurationResponse response = this.changeInvisibleDurationActivity.changeInvisibleDuration(
            createContext(),
            ChangeInvisibleDurationRequest.newBuilder()
                .setInvisibleDuration(Durations.fromSeconds(5))
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageId(msgId)
                .setReceiptHandle(oldHandle)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(newHandle, response.getReceiptHandle());
        assertEquals(newHandle, messageReceiptHandle.getReceiptHandleStr());
        verify(messagingProcessor).addReceiptHandle(any(), any(), eq(CONSUMER_GROUP), eq(msgId),
            eq(messageReceiptHandle));
    }

    @Test
    public void testChangeInvisibleDurationReusesOriginalChannelWhenClientDisconnectsBeforeCallback() throws Throwable {
        String oldHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000);
        String newHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 5000);
        String msgId = "msgId";
        Channel channel = grpcChannelManager.createChannel(createContext(), CLIENT_ID);
        MessageReceiptHandle messageReceiptHandle =
            new MessageReceiptHandle(CONSUMER_GROUP, TOPIC, 0, oldHandle, msgId, 1, 2);
        CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyBoolean()
        )).thenReturn(ackResultFuture);
        when(messagingProcessor.removeReceiptHandle(any(), eq(channel), anyString(), anyString(), anyString()))
            .thenReturn(messageReceiptHandle);

        CompletableFuture<ChangeInvisibleDurationResponse> responseFuture =
            this.changeInvisibleDurationActivity.changeInvisibleDuration(
                createContext(),
                ChangeInvisibleDurationRequest.newBuilder()
                    .setInvisibleDuration(Durations.fromSeconds(5))
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                    .setMessageId(msgId)
                    .setReceiptHandle(oldHandle)
                    .build()
            );

        grpcChannelManager.removeChannel(CLIENT_ID);
        AckResult ackResult = new AckResult();
        ackResult.setExtraInfo(newHandle);
        ackResult.setStatus(AckStatus.OK);
        ackResultFuture.complete(ackResult);

        assertEquals(Code.OK, responseFuture.get().getStatus().getCode());
        verify(messagingProcessor).addReceiptHandle(any(), eq(channel), eq(CONSUMER_GROUP), eq(msgId),
            eq(messageReceiptHandle));
    }

    @Test
    public void testChangeInvisibleDurationDoesNotRegisterNewHandleWhenOldHandleIsNotManaged() throws Throwable {
        String newHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 5000);
        AckResult ackResult = new AckResult();
        ackResult.setExtraInfo(newHandle);
        ackResult.setStatus(AckStatus.OK);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyBoolean()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));
        when(messagingProcessor.removeReceiptHandle(any(), any(), anyString(), anyString(), anyString()))
            .thenReturn(null);

        ChangeInvisibleDurationResponse response = this.changeInvisibleDurationActivity.changeInvisibleDuration(
            createContext(),
            ChangeInvisibleDurationRequest.newBuilder()
                .setInvisibleDuration(Durations.fromSeconds(5))
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageId("msgId")
                .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(newHandle, response.getReceiptHandle());
        verify(messagingProcessor, never()).addReceiptHandle(any(), any(), anyString(), anyString(),
            any(MessageReceiptHandle.class));
    }

    @Test
    public void testChangeInvisibleDurationRestoresOldHandleWhenBrokerReturnsNonOk() throws Throwable {
        String oldHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000);
        String msgId = "msgId";
        grpcChannelManager.createChannel(createContext(), CLIENT_ID);
        MessageReceiptHandle messageReceiptHandle =
            new MessageReceiptHandle(CONSUMER_GROUP, TOPIC, 0, oldHandle, msgId, 1, 2);
        AckResult ackResult = new AckResult();
        ackResult.setStatus(AckStatus.NO_EXIST);
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyBoolean()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));
        when(messagingProcessor.removeReceiptHandle(any(), any(), anyString(), anyString(), anyString()))
            .thenReturn(messageReceiptHandle);

        ChangeInvisibleDurationResponse response = this.changeInvisibleDurationActivity.changeInvisibleDuration(
            createContext(),
            ChangeInvisibleDurationRequest.newBuilder()
                .setInvisibleDuration(Durations.fromSeconds(5))
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .setMessageId(msgId)
                .setReceiptHandle(oldHandle)
                .build()
        ).get();

        assertEquals(Code.INTERNAL_SERVER_ERROR, response.getStatus().getCode());
        // The old handle was re-registered (restored) and left unmodified.
        assertEquals(oldHandle, messageReceiptHandle.getReceiptHandleStr());
        verify(messagingProcessor).addReceiptHandle(any(), any(), eq(CONSUMER_GROUP), eq(msgId),
            eq(messageReceiptHandle));
    }

    @Test
    public void testChangeInvisibleDurationRestoresOldHandleWhenBrokerFutureFails() throws Throwable {
        String oldHandle = buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000);
        String msgId = "msgId";
        grpcChannelManager.createChannel(createContext(), CLIENT_ID);
        MessageReceiptHandle messageReceiptHandle =
            new MessageReceiptHandle(CONSUMER_GROUP, TOPIC, 0, oldHandle, msgId, 1, 2);
        CompletableFuture<AckResult> ackResultFuture = new CompletableFuture<>();
        ackResultFuture.completeExceptionally(new RuntimeException("broker boom"));
        when(this.messagingProcessor.changeInvisibleTime(
            any(),
            any(),
            anyString(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyBoolean()
        )).thenReturn(ackResultFuture);
        when(messagingProcessor.removeReceiptHandle(any(), any(), anyString(), anyString(), anyString()))
            .thenReturn(messageReceiptHandle);

        try {
            this.changeInvisibleDurationActivity.changeInvisibleDuration(
                createContext(),
                ChangeInvisibleDurationRequest.newBuilder()
                    .setInvisibleDuration(Durations.fromSeconds(5))
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                    .setMessageId(msgId)
                    .setReceiptHandle(oldHandle)
                    .build()
            ).get();
            org.junit.Assert.fail("expected ExecutionException");
        } catch (ExecutionException executionException) {
            // expected: the failure surfaces to the caller
        }

        // The old handle was re-registered (restored) and left unmodified.
        assertEquals(oldHandle, messageReceiptHandle.getReceiptHandleStr());
        verify(messagingProcessor).addReceiptHandle(any(), any(), eq(CONSUMER_GROUP), eq(msgId),
            eq(messageReceiptHandle));
    }

    @Test
    public void testChangeInvisibleDurationInvisibleTimeTooSmall() throws Throwable {
        try {
            this.changeInvisibleDurationActivity.changeInvisibleDuration(
                createContext(),
                ChangeInvisibleDurationRequest.newBuilder()
                    .setInvisibleDuration(Durations.fromSeconds(-1))
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                    .setMessageId("msgId")
                    .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                    .build()
            ).get();
        } catch (ExecutionException executionException) {
            GrpcProxyException exception = (GrpcProxyException) executionException.getCause();
            assertEquals(Code.ILLEGAL_INVISIBLE_TIME, exception.getCode());
        }
    }

    @Test
    public void testChangeInvisibleDurationInvisibleTimeTooLarge() throws Throwable {
        try {
            this.changeInvisibleDurationActivity.changeInvisibleDuration(
                createContext(),
                ChangeInvisibleDurationRequest.newBuilder()
                    .setInvisibleDuration(Durations.fromDays(7))
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                    .setMessageId("msgId")
                    .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                    .build()
            ).get();
        } catch (ExecutionException executionException) {
            GrpcProxyException exception = (GrpcProxyException) executionException.getCause();
            assertEquals(Code.ILLEGAL_INVISIBLE_TIME, exception.getCode());
        }
    }
}
