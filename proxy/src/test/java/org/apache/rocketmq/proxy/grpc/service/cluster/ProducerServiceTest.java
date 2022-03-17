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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.grpc.Context;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.common.ProxyException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class ProducerServiceTest extends BaseServiceTest {

    private static final SendMessageRequest REQUEST = SendMessageRequest.newBuilder()
        .setMessage(Message.newBuilder()
            .setTopic(Resource.newBuilder()
                .setResourceNamespace("namespace")
                .setName("topic")
                .build())
            .setSystemAttribute(SystemAttribute.newBuilder()
                .setMessageId("msgId")
                .build())
            .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
            .build())
        .build();

    @Override
    public void beforeEach() throws Throwable {
    }

    @Test
    public void testSendMessage() {
        CompletableFuture<SendResult> sendResultFuture = new CompletableFuture<>();
        when(producerClient.sendMessage(anyString(), anyString(), any(), any(), anyLong()))
            .thenReturn(sendResultFuture);
        sendResultFuture.complete(new SendResult(SendStatus.SEND_OK, "msgId", new MessageQueue(),
            1L, "txId", "offsetMsgId", "regionId"));

        ProducerService producerService = new ProducerService(this.clientManager);
        producerService.setMessageQueueSelector((ctx, request, requestHeader, message) ->
            new SelectableMessageQueue(new MessageQueue("namespace%topic", "brokerName", 0), "brokerAddr"));

        CompletableFuture<SendMessageResponse> future = producerService.sendMessage(Context.current(), REQUEST);
        try {
            SendMessageResponse response = future.get();

            assertEquals(Code.OK.getNumber(), response.getCommon().getStatus().getCode());
            assertEquals("msgId", response.getMessageId());
        } catch (Exception e) {
            assertNull(e);
        }
    }

    @Test
    public void testSendMessageNoQueueSelect() {
        ProducerService producerService = new ProducerService(this.clientManager);

        producerService.setMessageQueueSelector((ctx, request, requestHeader, message) -> null);

        CompletableFuture<SendMessageResponse> future = producerService.sendMessage(Context.current(), SendMessageRequest.newBuilder()
            .setMessage(Message.newBuilder()
                .setTopic(Resource.newBuilder()
                    .setResourceNamespace("namespace")
                    .setName("topic")
                    .build())
                .setSystemAttribute(SystemAttribute.newBuilder()
                    .setMessageId("msgId")
                    .build())
                .setBody(ByteString.copyFrom("hello", StandardCharsets.UTF_8))
                .build())
            .build());

        try {
            SendMessageResponse response = future.get();
            assertNull(response);
        } catch (Exception e) {
            assertNotNull(e);
            assertTrue(e instanceof ExecutionException);
            assertTrue(e.getCause() instanceof ProxyException);
            assertEquals(Code.NOT_FOUND, ((ProxyException) e.getCause()).getCode());
        }
    }

    @Test
    public void testSendMessageWithError() {
        RuntimeException ex = new RuntimeException();

        CompletableFuture<SendResult> sendResultFuture = new CompletableFuture<>();
        when(producerClient.sendMessage(anyString(), anyString(), any(), any(), anyLong()))
            .thenReturn(sendResultFuture);
        sendResultFuture.completeExceptionally(ex);

        ProducerService producerService = new ProducerService(this.clientManager);
        producerService.setMessageQueueSelector((ctx, request, requestHeader, message) ->
            new SelectableMessageQueue(new MessageQueue("namespace%topic", "brokerName", 0), "brokerAddr"));

        CompletableFuture<SendMessageResponse> future = producerService.sendMessage(Context.current(), REQUEST);

        try {
            SendMessageResponse response = future.get();
            assertNull(response);
        } catch (Exception e) {
            assertNotNull(e);
            assertTrue(e instanceof ExecutionException);
            assertSame(ex, e.getCause());
        }
    }

    @Test
    public void testSendMessageWithErrorThrow() {
        RuntimeException ex = new RuntimeException();

        ProducerService producerService = new ProducerService(this.clientManager);
        producerService.setMessageQueueSelector((ctx, request, requestHeader, message) -> {
            throw ex;
        });
        producerService.setSendMessageHook((request, response, t) ->  {
            assertSame(ex, t);
        });

        CompletableFuture<SendMessageResponse> future = producerService.sendMessage(Context.current(), REQUEST);

        try {
            SendMessageResponse response = future.get();
            assertNull(response);
        } catch (Exception e) {
            assertNotNull(e);
            assertTrue(e instanceof ExecutionException);
            assertSame(ex, e.getCause());
        }
    }
}