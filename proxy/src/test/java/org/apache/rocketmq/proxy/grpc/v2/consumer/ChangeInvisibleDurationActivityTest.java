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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class ChangeInvisibleDurationActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private ChangeInvisibleDurationActivity changeInvisibleDurationActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.changeInvisibleDurationActivity = new ChangeInvisibleDurationActivity(messagingProcessor, receiptHandleProcessor,
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
            any(), any(), anyString(), anyString(), anyString(), invisibleTimeArgumentCaptor.capture()
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
            any(), receiptHandleCaptor.capture(), anyString(), anyString(), anyString(), invisibleTimeArgumentCaptor.capture()
        )).thenReturn(CompletableFuture.completedFuture(ackResult));
        when(receiptHandleProcessor.removeReceiptHandle(anyString(), anyString(), anyString(), anyString()))
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
            any(), any(), anyString(), anyString(), anyString(), invisibleTimeArgumentCaptor.capture()
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