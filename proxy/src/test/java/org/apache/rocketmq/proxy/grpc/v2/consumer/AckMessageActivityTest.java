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
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Resource;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class AckMessageActivityTest extends BaseActivityTest {

    private AckMessageActivity ackMessageActivity;

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";

    @Before
    public void before() throws Throwable {
        super.before();
        this.ackMessageActivity = new AckMessageActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testAckMessage() throws Throwable {
        when(this.messagingProcessor.ackMessage(any(), any(), eq("msg1"), anyString(), anyString()))
            .thenThrow(new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "receipt handle is expired"));

        AckResult msg2AckResult = new AckResult();
        msg2AckResult.setStatus(AckStatus.OK);
        when(this.messagingProcessor.ackMessage(any(), any(), eq("msg2"), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(msg2AckResult));

        AckResult msg3AckResult = new AckResult();
        msg3AckResult.setStatus(AckStatus.NO_EXIST);
        when(this.messagingProcessor.ackMessage(any(), any(), eq("msg3"), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(msg3AckResult));

        AckMessageResponse response = this.ackMessageActivity.ackMessage(
            createContext(),
            AckMessageRequest.newBuilder()
                .setTopic(Resource.newBuilder().setName(TOPIC).build())
                .setGroup(Resource.newBuilder().setName(GROUP).build())
                .addEntries(AckMessageEntry.newBuilder()
                    .setMessageId("msg1")
                    .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis() - 10000, 1000))
                    .build())
                .addEntries(AckMessageEntry.newBuilder()
                    .setMessageId("msg2")
                    .setReceiptHandle(buildReceiptHandle(TOPIC, System.currentTimeMillis(), 3000))
                    .build())
                .addEntries(AckMessageEntry.newBuilder()
                    .setMessageId("msg3")
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