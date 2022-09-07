/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.Resource;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class ForwardMessageToDLQActivityTest extends BaseActivityTest {

    private ForwardMessageToDLQActivity forwardMessageToDLQActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.forwardMessageToDLQActivity = new ForwardMessageToDLQActivity(messagingProcessor,receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testForwardMessageToDeadLetterQueue() throws Throwable {
        ArgumentCaptor<ReceiptHandle> receiptHandleCaptor = ArgumentCaptor.forClass(ReceiptHandle.class);
        when(this.messagingProcessor.forwardMessageToDeadLetterQueue(any(), receiptHandleCaptor.capture(), anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "")));

        String handleStr = buildReceiptHandle("topic", System.currentTimeMillis(), 3000);
        ForwardMessageToDeadLetterQueueResponse response = this.forwardMessageToDLQActivity.forwardMessageToDeadLetterQueue(
            createContext(),
            ForwardMessageToDeadLetterQueueRequest.newBuilder()
                .setTopic(Resource.newBuilder().setName("topic").build())
                .setGroup(Resource.newBuilder().setName("group").build())
                .setMessageId(MessageClientIDSetter.createUniqID())
                .setReceiptHandle(handleStr)
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(handleStr, receiptHandleCaptor.getValue().getReceiptHandle());
    }

    @Test
    public void testForwardMessageToDeadLetterQueueWhenHasMappingHandle() throws Throwable {
        ArgumentCaptor<ReceiptHandle> receiptHandleCaptor = ArgumentCaptor.forClass(ReceiptHandle.class);
        when(this.messagingProcessor.forwardMessageToDeadLetterQueue(any(), receiptHandleCaptor.capture(), anyString(), anyString(), anyString()))
            .thenReturn(CompletableFuture.completedFuture(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "")));

        String savedHandleStr = buildReceiptHandle("topic", System.currentTimeMillis(),3000);
        when(receiptHandleProcessor.removeReceiptHandle(anyString(), anyString(), anyString(), anyString()))
            .thenReturn(new MessageReceiptHandle("group", "topic", 0, savedHandleStr, "msgId", 0, 0));

        ForwardMessageToDeadLetterQueueResponse response = this.forwardMessageToDLQActivity.forwardMessageToDeadLetterQueue(
            createContext(),
            ForwardMessageToDeadLetterQueueRequest.newBuilder()
                .setTopic(Resource.newBuilder().setName("topic").build())
                .setGroup(Resource.newBuilder().setName("group").build())
                .setMessageId(MessageClientIDSetter.createUniqID())
                .setReceiptHandle(buildReceiptHandle("topic", System.currentTimeMillis(), 3000))
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(savedHandleStr, receiptHandleCaptor.getValue().getReceiptHandle());
    }
}