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

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.processor.BatchAckResult;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class AckMessageActivityTest extends BaseActivityTest {

    private AckMessageActivity ackMessageActivity;

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";

    @Before
    public void before() throws Throwable {
        super.before();
        this.ackMessageActivity = new AckMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testAckMessage() throws Throwable {
        ConfigurationManager.getProxyConfig().setEnableBatchAck(false);

        String msg1 = "msg1";
        String msg2 = "msg2";
        String msg3 = "msg3";

        when(this.messagingProcessor.ackMessage(any(), any(), eq(msg1), anyString(), anyString()))
            .thenThrow(new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "receipt handle is expired"));

        AckResult msg2AckResult = new AckResult();
        msg2AckResult.setStatus(AckStatus.OK);
        when(this.messagingProcessor.ackMessage(any(), any(), eq(msg2), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(msg2AckResult));

        AckResult msg3AckResult = new AckResult();
        msg3AckResult.setStatus(AckStatus.NO_EXIST);
        when(this.messagingProcessor.ackMessage(any(), any(), eq(msg3), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(msg3AckResult));

        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(msg1)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis() - 10000, 1000))
                        .build())
                    .build()
            ).get();
            assertEquals(Code.INVALID_RECEIPT_HANDLE, response.getStatus().getCode());
        }
        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(msg2)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis() - 10000, 1000))
                        .build())
                    .build()
            ).get();
            assertEquals(Code.OK, response.getStatus().getCode());
        }
        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(msg3)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis() - 10000, 1000))
                        .build())
                    .build()
            ).get();
            assertEquals(Code.INTERNAL_SERVER_ERROR, response.getStatus().getCode());
        }
        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(msg1)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis() - 10000, 1000))
                        .build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(msg2)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(msg3)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .build()
            ).get();

            assertEquals(Code.MULTIPLE_RESULTS, response.getStatus().getCode());
            assertEquals(3, response.getEntriesCount());
            assertEquals(Code.INVALID_RECEIPT_HANDLE, response.getEntries(0).getStatus().getCode());
            assertEquals(Code.OK, response.getEntries(1).getStatus().getCode());
            assertEquals(Code.INTERNAL_SERVER_ERROR, response.getEntries(2).getStatus().getCode());
        }
    }

    @Test
    public void testAckMessageInBatch() throws Throwable {
        ConfigurationManager.getProxyConfig().setEnableBatchAck(true);

        String successMessageId = "msg1";
        String notOkMessageId = "msg2";
        String exceptionMessageId = "msg3";

        doAnswer((Answer<CompletableFuture<List<BatchAckResult>>>) invocation -> {
            List<ReceiptHandleMessage> receiptHandleMessageList = invocation.getArgument(1, List.class);
            List<BatchAckResult> batchAckResultList = new ArrayList<>();
            for (ReceiptHandleMessage receiptHandleMessage : receiptHandleMessageList) {
                BatchAckResult batchAckResult;
                if (receiptHandleMessage.getMessageId().equals(successMessageId)) {
                    AckResult ackResult = new AckResult();
                    ackResult.setStatus(AckStatus.OK);
                    batchAckResult = new BatchAckResult(receiptHandleMessage, ackResult);
                } else if (receiptHandleMessage.getMessageId().equals(notOkMessageId)) {
                    AckResult ackResult = new AckResult();
                    ackResult.setStatus(AckStatus.NO_EXIST);
                    batchAckResult = new BatchAckResult(receiptHandleMessage, ackResult);
                } else {
                    batchAckResult = new BatchAckResult(receiptHandleMessage, new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, ""));
                }
                batchAckResultList.add(batchAckResult);
            }
            return CompletableFuture.completedFuture(batchAckResultList);
        }).when(this.messagingProcessor).batchAckMessage(any(), anyList(), anyString(), anyString());

        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(successMessageId)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .build()
            ).get();
            assertEquals(Code.OK, response.getStatus().getCode());
        }
        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(notOkMessageId)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .build()
            ).get();
            assertEquals(Code.INTERNAL_SERVER_ERROR, response.getStatus().getCode());
        }
        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(exceptionMessageId)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .build()
            ).get();
            assertEquals(Code.INVALID_RECEIPT_HANDLE, response.getStatus().getCode());
        }
        {
            AckMessageResponse response = this.ackMessageActivity.ackMessage(
                createContext(),
                AckMessageRequest.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .setGroup(Resource.newBuilder().setName(GROUP).build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(successMessageId)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(notOkMessageId)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .addEntries(AckMessageEntry.newBuilder()
                        .setMessageId(exceptionMessageId)
                        .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                        .build())
                    .build()
            ).get();

            assertEquals(Code.MULTIPLE_RESULTS, response.getStatus().getCode());
            assertEquals(3, response.getEntriesCount());
            Map<String, Code> msgCode = new HashMap<>();
            for (AckMessageResultEntry entry : response.getEntriesList()) {
                msgCode.put(entry.getMessageId(), entry.getStatus().getCode());
            }
            assertEquals(Code.OK, msgCode.get(successMessageId));
            assertEquals(Code.INTERNAL_SERVER_ERROR, msgCode.get(notOkMessageId));
            assertEquals(Code.INVALID_RECEIPT_HANDLE, msgCode.get(exceptionMessageId));
        }
    }
}