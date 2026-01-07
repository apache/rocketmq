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

package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteSubscriptionRegistry;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.lite.LiteSubscriptionAction;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteSubscriptionCtlProcessorTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private LiteSubscriptionRegistry liteSubscriptionRegistry;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @InjectMocks
    private LiteSubscriptionCtlProcessor processor;

    @Test
    public void testProcessRequest_BodyIsNull() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        RemotingCommand response = processor.processRequest(ctx, request);
        assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());
    }

    @Test
    public void testProcessRequest_SubscriptionSetIsEmpty() throws Exception {
        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(Collections.emptySet());
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setBody(requestBody.encode());
        RemotingCommand response = processor.processRequest(ctx, request);
        assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());
    }

    @Test
    public void testProcessRequest_ActionIsIncrementalAdd() throws Exception {
        String clientId = "clientId";
        String group = "group";
        String topic = "topic";
        String liteTopic = "liteTopic";
        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        LiteSubscriptionDTO dto = new LiteSubscriptionDTO();
        dto.setClientId(clientId);
        dto.setGroup(group);
        dto.setTopic(topic);
        dto.setLiteTopicSet(liteTopicSet);
        dto.setAction(LiteSubscriptionAction.PARTIAL_ADD);
        dto.setVersion(1L);

        Set<LiteSubscriptionDTO> subscriptionSet = new HashSet<>();
        subscriptionSet.add(dto);

        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(subscriptionSet);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setBody(requestBody.encode());

        when(ctx.channel()).thenReturn(channel);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(true);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        RemotingCommand response = processor.processRequest(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        verify(liteSubscriptionRegistry).updateClientChannel(eq(clientId), eq(channel));
        verify(liteSubscriptionRegistry).addPartialSubscription(eq(clientId), eq(group), eq(topic), anySet(), any());
    }

    @Test
    public void testProcessRequest_ActionIsAllAdd() throws Exception {
        String clientId = "clientId";
        String group = "group";
        String topic = "topic";
        String liteTopic = "liteTopic";
        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        LiteSubscriptionDTO dto = new LiteSubscriptionDTO();
        dto.setClientId(clientId);
        dto.setGroup(group);
        dto.setTopic(topic);
        dto.setLiteTopicSet(liteTopicSet);
        dto.setAction(LiteSubscriptionAction.COMPLETE_ADD);
        dto.setVersion(1L);

        Set<LiteSubscriptionDTO> subscriptionSet = new HashSet<>();
        subscriptionSet.add(dto);

        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(subscriptionSet);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setBody(requestBody.encode());

        when(ctx.channel()).thenReturn(channel);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(true);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        RemotingCommand response = processor.processRequest(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        verify(liteSubscriptionRegistry).updateClientChannel(eq(clientId), eq(channel));
        verify(liteSubscriptionRegistry).addCompleteSubscription(eq(clientId), eq(group), eq(topic), anySet(), eq(1L));
    }

    @Test
    public void testProcessRequest_ActionIsIncrementalRemove() throws Exception {
        String clientId = "clientId";
        String group = "group";
        String topic = "topic";
        String liteTopic = "liteTopic";
        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        LiteSubscriptionDTO dto = new LiteSubscriptionDTO();
        dto.setClientId(clientId);
        dto.setGroup(group);
        dto.setTopic(topic);
        dto.setLiteTopicSet(liteTopicSet);
        dto.setAction(LiteSubscriptionAction.PARTIAL_REMOVE);

        Set<LiteSubscriptionDTO> subscriptionSet = new HashSet<>();
        subscriptionSet.add(dto);

        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(subscriptionSet);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setBody(requestBody.encode());

        RemotingCommand response = processor.processRequest(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        verify(liteSubscriptionRegistry).removePartialSubscription(eq(clientId), eq(group), eq(topic), anySet());
    }

    @Test
    public void testProcessRequest_ActionIsAllRemove() throws Exception {
        String clientId = "clientId";

        LiteSubscriptionDTO dto = new LiteSubscriptionDTO();
        String group = "group";
        String topic = "topic";
        dto.setClientId(clientId);
        dto.setTopic(topic);
        dto.setGroup(group);
        dto.setAction(LiteSubscriptionAction.COMPLETE_REMOVE);

        Set<LiteSubscriptionDTO> subscriptionSet = new HashSet<>();
        subscriptionSet.add(dto);

        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(subscriptionSet);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setBody(requestBody.encode());

        RemotingCommand response = processor.processRequest(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        verify(liteSubscriptionRegistry).removeCompleteSubscription(eq(clientId));
    }

    @Test
    public void testProcessRequest_CheckConsumeEnableThrowsException() throws Exception {
        String clientId = "clientId";
        String group = "group";
        String topic = "topic";
        String liteTopic = "liteTopic";
        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add(liteTopic);

        LiteSubscriptionDTO dto = new LiteSubscriptionDTO();
        dto.setClientId(clientId);
        dto.setGroup(group);
        dto.setTopic(topic);
        dto.setLiteTopicSet(liteTopicSet);
        dto.setAction(LiteSubscriptionAction.PARTIAL_ADD);

        Set<LiteSubscriptionDTO> subscriptionSet = new HashSet<>();
        subscriptionSet.add(dto);

        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(subscriptionSet);
        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        request.setBody(requestBody.encode());

        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setConsumeEnable(false);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        RemotingCommand response = processor.processRequest(ctx, request);

        assertEquals(ResponseCode.ILLEGAL_OPERATION, response.getCode());
        assertTrue(response.getRemark().contains("Consumer group is not allowed to consume."));
    }

}
