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
package org.apache.rocketmq.proxy.remoting.activity;

import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class ConsumerManagerActivityTest {

    @Test
    public void testLockBatchMQWithEmptyQueueSetReturnsError() throws Exception {
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        ConsumerManagerActivity activity = new ConsumerManagerActivity(null, messagingProcessor);
        LockBatchRequestBody body = new LockBatchRequestBody();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(body.encode());

        RemotingCommand response = activity.lockBatchMQ(null, request, ProxyContext.create());

        assertNotNull(response);
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
        assertEquals("MessageQueue set is empty", response.getRemark());
        verifyNoInteractions(messagingProcessor);
    }

    @Test
    public void testUnlockBatchMQWithEmptyQueueSetReturnsError() throws Exception {
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        ConsumerManagerActivity activity = new ConsumerManagerActivity(null, messagingProcessor);
        UnlockBatchRequestBody body = new UnlockBatchRequestBody();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        request.setBody(body.encode());

        RemotingCommand response = activity.unlockBatchMQ(null, request, ProxyContext.create());

        assertNotNull(response);
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
        assertEquals("MessageQueue set is empty", response.getRemark());
        verifyNoInteractions(messagingProcessor);
    }
}
