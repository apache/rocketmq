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

package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.Resource;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

public class RecallMessageActivityTest extends BaseActivityTest {
    private RecallMessageActivity recallMessageActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.recallMessageActivity =
            new RecallMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testRecallMessage_success() {
        when(this.messagingProcessor.recallMessage(any(), any(), any(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture("msgId"));

        RecallMessageResponse response = this.recallMessageActivity.recallMessage(
            createContext(),
            RecallMessageRequest.newBuilder()
                .setRecallHandle("handle")
                .setTopic(Resource.newBuilder().setResourceNamespace("ns").setName("topic"))
                .build()
        ).join();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals("msgId", response.getMessageId());
    }

    @Test
    public void testRecallMessage_fail() {
        CompletableFuture<String> exceptionFuture = new CompletableFuture();
        when(this.messagingProcessor.recallMessage(any(), any(), any(), anyLong())).thenReturn(exceptionFuture);
        exceptionFuture.completeExceptionally(
            new ProxyException(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, "info"));

        CompletionException exception = Assert.assertThrows(CompletionException.class, () -> {
            this.recallMessageActivity.recallMessage(
                createContext(),
                RecallMessageRequest.newBuilder()
                    .setRecallHandle("handle")
                    .setTopic(Resource.newBuilder().setResourceNamespace("ns").setName("topic"))
                    .build()
            ).join();
        });
        Assert.assertTrue(exception.getCause() instanceof ProxyException);
        ProxyException cause = (ProxyException) exception.getCause();
        Assert.assertEquals(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, cause.getCode());
    }
}
