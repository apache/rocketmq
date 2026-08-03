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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerManagerActivityTest extends InitConfigTest {

    private ConsumerManagerActivity consumerManagerActivity;
    private ChannelHandlerContext ctx;

    @Mock
    private MessagingProcessor messagingProcessor;

    @Before
    public void setup() {
        consumerManagerActivity = new ConsumerManagerActivity(null, messagingProcessor);
        ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "0.0.0.0:0", "1.1.1.1:1"));
    }

    @Test
    public void testLockBatchMQShouldRejectEmptyMessageQueueSet() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(new LockBatchRequestBody().encode());

        RemotingCommand response = consumerManagerActivity.processRequest0(ctx, request, ProxyContext.create());

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("MessageQueue set is empty");
        verify(messagingProcessor, never()).request(any(), any(), any(), anyLong());
    }

    @Test
    public void testUnlockBatchMQShouldRejectEmptyMessageQueueSet() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        request.setBody(new UnlockBatchRequestBody().encode());

        RemotingCommand response = consumerManagerActivity.processRequest0(ctx, request, ProxyContext.create());

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("MessageQueue set is empty");
        verify(messagingProcessor, never()).request(any(), any(), any(), anyLong());
    }
}
