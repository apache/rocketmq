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

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.GetOffsetRequest;
import apache.rocketmq.v2.GetOffsetResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryOffsetPolicy;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.Resource;
import com.google.protobuf.util.Timestamps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class OffsetActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final String BROKER_NAME = "brokerName";
    private static final long OFFSET = 123L;
    private static final long TIMESTAMP = 1000L;

    private OffsetActivity offsetActivity;
    private MessageQueue messageQueue;
    private org.apache.rocketmq.common.message.MessageQueue commonMessageQueue;

    @Before
    public void before() throws Throwable {
        super.before();
        this.offsetActivity = new OffsetActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.messageQueue = MessageQueue.newBuilder()
            .setTopic(Resource.newBuilder().setName(TOPIC).build())
            .setBroker(Broker.newBuilder().setName(BROKER_NAME).build())
            .setId(0)
            .build();
        this.commonMessageQueue = new org.apache.rocketmq.common.message.MessageQueue(TOPIC, BROKER_NAME, 0);
    }

    @Test
    public void testGetOffset() throws Throwable {
        ArgumentCaptor<org.apache.rocketmq.common.message.MessageQueue> messageQueueCaptor =
            ArgumentCaptor.forClass(org.apache.rocketmq.common.message.MessageQueue.class);
        when(this.messagingProcessor.queryConsumerOffset(
            any(),
            messageQueueCaptor.capture(),
            eq(GROUP),
            eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS)
        )).thenReturn(CompletableFuture.completedFuture(OFFSET));

        GetOffsetResponse response = this.offsetActivity.getOffset(
            createContext(),
            GetOffsetRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(GROUP).build())
                .setMessageQueue(messageQueue)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(OFFSET, response.getOffset());
        assertEquals(commonMessageQueue, messageQueueCaptor.getValue());
    }

    @Test
    public void testQueryOffsetBeginning() throws Throwable {
        when(this.messagingProcessor.getMinOffset(any(), eq(commonMessageQueue), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(OFFSET));

        QueryOffsetResponse response = this.offsetActivity.queryOffset(
            createContext(),
            QueryOffsetRequest.newBuilder()
                .setMessageQueue(messageQueue)
                .setQueryOffsetPolicy(QueryOffsetPolicy.BEGINNING)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(OFFSET, response.getOffset());
    }

    @Test
    public void testQueryOffsetEnd() throws Throwable {
        when(this.messagingProcessor.getMaxOffset(any(), eq(commonMessageQueue), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(OFFSET));

        QueryOffsetResponse response = this.offsetActivity.queryOffset(
            createContext(),
            QueryOffsetRequest.newBuilder()
                .setMessageQueue(messageQueue)
                .setQueryOffsetPolicy(QueryOffsetPolicy.END)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(OFFSET, response.getOffset());
    }

    @Test
    public void testQueryOffsetTimestamp() throws Throwable {
        ArgumentCaptor<Long> timestampCaptor = ArgumentCaptor.forClass(Long.class);
        when(this.messagingProcessor.searchOffset(
            any(),
            eq(commonMessageQueue),
            timestampCaptor.capture(),
            eq(MessagingProcessor.DEFAULT_TIMEOUT_MILLS)
        )).thenReturn(CompletableFuture.completedFuture(OFFSET));

        QueryOffsetResponse response = this.offsetActivity.queryOffset(
            createContext(),
            QueryOffsetRequest.newBuilder()
                .setMessageQueue(messageQueue)
                .setQueryOffsetPolicy(QueryOffsetPolicy.TIMESTAMP)
                .setTimestamp(Timestamps.fromMillis(TIMESTAMP))
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(OFFSET, response.getOffset());
        assertEquals(TIMESTAMP, timestampCaptor.getValue().longValue());
    }

    @Test
    public void testQueryOffsetTimestampWithoutTimestamp() throws Throwable {
        try {
            this.offsetActivity.queryOffset(
                createContext(),
                QueryOffsetRequest.newBuilder()
                    .setMessageQueue(messageQueue)
                    .setQueryOffsetPolicy(QueryOffsetPolicy.TIMESTAMP)
                    .build()
            ).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof GrpcProxyException);
            GrpcProxyException grpcProxyException = (GrpcProxyException) e.getCause();
            assertEquals(Code.BAD_REQUEST, grpcProxyException.getCode());
            return;
        }
        throw new AssertionError("Expected GrpcProxyException");
    }
}
