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

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannelManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(MockitoJUnitRunner.class)
public class ClientManagerActivityTest extends InitConfigTest {
    private ClientManagerActivity clientManagerActivity;

    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private RemotingChannelManager remotingChannelManager;

    @Before
    public void setup() {
        this.clientManagerActivity = new ClientManagerActivity(null, messagingProcessor, remotingChannelManager);
    }

    @Test
    public void testHeartbeatShouldRejectEmptyBody() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);

        RemotingCommand response = clientManagerActivity.heartBeat(null, request, ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.INVALID_PARAMETER);
        assertThat(response.getRemark()).isEqualTo("heartbeat data is empty");
        verifyNoClientRegistration();
    }

    @Test
    public void testHeartbeatShouldRejectNullDataSets() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody("{\"clientID\":\"client-a\",\"producerDataSet\":null,\"consumerDataSet\":[]}"
            .getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = clientManagerActivity.heartBeat(null, request, ProxyContext.create());

        assertThat(response.getCode()).isEqualTo(ResponseCode.INVALID_PARAMETER);
        assertThat(response.getRemark()).isEqualTo("heartbeat producerDataSet and consumerDataSet are required");
        verifyNoClientRegistration();
    }

    private void verifyNoClientRegistration() {
        verify(messagingProcessor, never()).registerProducer(any(), any(), any());
        verify(messagingProcessor, never()).registerConsumer(any(), any(), any(), any(), any(), any(), any(), anyBoolean());
        verifyNoInteractions(remotingChannelManager);
    }
}
