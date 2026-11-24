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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PeekMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PeekMessageProcessorTest {

    private PeekMessageProcessor peekMessageProcessor;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private MessageStore messageStore;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Mock
    private SubscriptionGroupConfig subscriptionGroupConfig;

    @Mock
    private Channel channel;

    private TopicConfigManager topicConfigManager;

    @Before
    public void init() {
        // Initialize BrokerMetricsManager to prevent NPE in tests
        brokerController.setBrokerMetricsManager(new BrokerMetricsManager(brokerController));
        peekMessageProcessor = new PeekMessageProcessor(brokerController);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        topicConfigManager = new TopicConfigManager(brokerController);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(subscriptionGroupConfig);
        when(subscriptionGroupConfig.isConsumeEnable()).thenReturn(true);
        topicConfigManager.getTopicConfigTable().put("topic", new TopicConfig("topic"));
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.queryOffset(anyString(), anyString(), anyInt())).thenReturn(-1L);
        when(messageStore.getMinOffsetInQueue(anyString(),anyInt())).thenReturn(0L);
        when(handlerContext.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
    }

    @Test
    public void testProcessRequest() throws RemotingCommandException {
        RemotingCommand request = createPeekMessageRequest("group","topic",0);
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);
        ByteBuffer bb = ByteBuffer.allocate(64);
        bb.putLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION, System.currentTimeMillis());
        SelectMappedBufferResult mappedBufferResult1 = new SelectMappedBufferResult(0, bb, 64, null);
        for (int i = 0; i < 10;i++) {
            getMessageResult.addMessage(mappedBufferResult1);
        }
        when(messageStore.getMessage(anyString(),anyString(),anyInt(),anyLong(),anyInt(),any())).thenReturn(getMessageResult);
        RemotingCommand response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_NoPermission() throws RemotingCommandException {
        this.brokerController.getBrokerConfig().setBrokerPermission(PermName.PERM_WRITE);
        RemotingCommand request = createPeekMessageRequest("group","topic",0);
        RemotingCommand response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        this.brokerController.getBrokerConfig().setBrokerPermission(PermName.PERM_WRITE | PermName.PERM_READ);

        topicConfigManager.getTopicConfigTable().get("topic").setPerm(PermName.PERM_WRITE);
        response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        topicConfigManager.getTopicConfigTable().get("topic").setPerm(PermName.PERM_WRITE | PermName.PERM_READ);

        when(subscriptionGroupConfig.isConsumeEnable()).thenReturn(false);
        response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testProcessRequest_TopicNotExist() throws RemotingCommandException {
        RemotingCommand request = createPeekMessageRequest("group1","topic1",0);
        RemotingCommand response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
    }

    @Test
    public void testProcessRequest_SubscriptionGroupNotExist() throws RemotingCommandException {
        when(subscriptionGroupManager.findSubscriptionGroupConfig(anyString())).thenReturn(null);
        RemotingCommand request = createPeekMessageRequest("group","topic",0);
        RemotingCommand response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
    }

    @Test
    public void testProcessRequest_QueueIdError() throws RemotingCommandException {
        RemotingCommand request = createPeekMessageRequest("group","topic",17);
        RemotingCommand response = peekMessageProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.INVALID_PARAMETER);
    }

    private RemotingCommand createPeekMessageRequest(String group,String topic,int queueId) {
        PeekMessageRequestHeader peekMessageRequestHeader = new PeekMessageRequestHeader();
        peekMessageRequestHeader.setConsumerGroup(group);
        peekMessageRequestHeader.setTopic(topic);
        peekMessageRequestHeader.setMaxMsgNums(10);
        peekMessageRequestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PEEK_MESSAGE, peekMessageRequestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}
