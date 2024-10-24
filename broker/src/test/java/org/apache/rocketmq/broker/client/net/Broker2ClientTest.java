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
package org.apache.rocketmq.broker.client.net;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.MessageStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Broker2ClientTest {
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private RemotingServer remotingServer;
    
    @Mock
    private ConsumerManager consumerManager;
    
    @Mock
    private TopicConfigManager topicConfigManager;
    
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    
    @Mock
    private Channel channel;
    
    @Mock
    private ConsumerGroupInfo consumerGroupInfo;
    
    private Broker2Client broker2Client;
    
    private final String defaultTopic = "defaultTopic";
    
    private final String defaultBroker = "defaultBroker";
    
    private final String defaultGroup = "defaultGroup";
    
    private final long timestamp = System.currentTimeMillis();
    
    private final boolean isForce = true;
    
    @Before
    public void init() {
        broker2Client = new Broker2Client(brokerController);
        when(brokerController.getRemotingServer()).thenReturn(remotingServer);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(brokerController.getBrokerConfig()).thenReturn(mock(BrokerConfig.class));
        when(brokerController.getMessageStore()).thenReturn(mock(MessageStore.class));
        when(consumerManager.getConsumerGroupInfo(any())).thenReturn(consumerGroupInfo);
    }
    
    @Test
    public void testCheckProducerTransactionState() throws Exception {
        CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        broker2Client.checkProducerTransactionState("group", channel, requestHeader, createMessageExt());
        verify(remotingServer).invokeOneway(eq(channel), any(RemotingCommand.class), eq(10L));
    }
    
    @Test
    public void testCheckProducerTransactionStateException() throws Exception {
        CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        MessageExt messageExt = createMessageExt();
        doThrow(new RuntimeException("Test Exception"))
                .when(remotingServer)
                .invokeOneway(any(Channel.class),
                        any(RemotingCommand.class),
                        anyLong());
        broker2Client.checkProducerTransactionState("group", channel, requestHeader, messageExt);
        verify(brokerController.getRemotingServer()).invokeOneway(eq(channel), any(RemotingCommand.class), eq(10L));
    }
    
    @Test
    public void testResetOffsetNoTopicConfig() throws RemotingCommandException {
        when(topicConfigManager.selectTopicConfig(defaultTopic)).thenReturn(null);
        RemotingCommand response = broker2Client.resetOffset(defaultTopic, defaultGroup, timestamp, isForce);
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
    }
    
    @Test
    public void testResetOffsetNoConsumerGroupInfo() throws RemotingCommandException {
        TopicConfig topicConfig = mock(TopicConfig.class);
        when(topicConfigManager.selectTopicConfig(defaultTopic)).thenReturn(topicConfig);
        when(topicConfig.getWriteQueueNums()).thenReturn(1);
        when(consumerOffsetManager.queryOffset(defaultGroup, defaultTopic, 0)).thenReturn(0L);
        RemotingCommand response = broker2Client.resetOffset(defaultTopic, defaultGroup, timestamp, isForce);
        assertEquals(ResponseCode.CONSUMER_NOT_ONLINE, response.getCode());
    }
    
    @Test
    public void testResetOffset() throws RemotingCommandException {
        TopicConfig topicConfig = mock(TopicConfig.class);
        when(topicConfigManager.selectTopicConfig(defaultTopic)).thenReturn(topicConfig);
        when(topicConfig.getWriteQueueNums()).thenReturn(1);
        when(brokerController.getConsumerOffsetManager().queryOffset(defaultGroup, defaultTopic, 0)).thenReturn(0L);
        BrokerConfig brokerConfig = mock(BrokerConfig.class);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerConfig.getBrokerName()).thenReturn(defaultBroker);
        ConsumerGroupInfo consumerGroupInfo = mock(ConsumerGroupInfo.class);
        when(consumerManager.getConsumerGroupInfo(defaultGroup)).thenReturn(consumerGroupInfo);
        RemotingCommand response = broker2Client.resetOffset(defaultTopic, defaultGroup, timestamp, isForce);
        assertEquals(ResponseCode.CONSUMER_NOT_ONLINE, response.getCode());
    }
    
    @Test
    public void testGetConsumeStatusNoConsumerOnline() {
        when(consumerGroupInfo.getChannelInfoTable()).thenReturn(new ConcurrentHashMap<>());
        RemotingCommand response = broker2Client.getConsumeStatus(defaultTopic, defaultGroup, "");
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
    }
    
    @Test
    public void testGetConsumeStatusClientDoesNotSupportFeature() {
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, "defaultClientId", null, MQVersion.Version.V3_0_6.ordinal());
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>();
        channelInfoTable.put(channel, clientChannelInfo);
        when(consumerGroupInfo.getChannelInfoTable()).thenReturn(channelInfoTable);
        RemotingCommand response = broker2Client.getConsumeStatus(defaultTopic, defaultGroup, "");
        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
    }
    
    @Test
    public void testGetConsumeStatus() throws Exception {
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>();
        ClientChannelInfo clientChannelInfo = mock(ClientChannelInfo.class);
        when(clientChannelInfo.getVersion()).thenReturn(MQVersion.CURRENT_VERSION);
        channelInfoTable.put(channel, clientChannelInfo);
        when(consumerGroupInfo.getChannelInfoTable()).thenReturn(channelInfoTable);
        RemotingCommand responseMock = mock(RemotingCommand.class);
        when(responseMock.getCode()).thenReturn(ResponseCode.SUCCESS);
        when(responseMock.getBody()).thenReturn("{\"consumerTable\":{}}".getBytes(StandardCharsets.UTF_8));
        when(remotingServer.invokeSync(any(Channel.class), any(RemotingCommand.class), anyLong())).thenReturn(responseMock);
        RemotingCommand response = broker2Client.getConsumeStatus(defaultTopic, defaultGroup, "");
        assertEquals(ResponseCode.SUCCESS, response.getCode());
        GetConsumerStatusBody body = RemotingSerializable.decode(response.getBody(), GetConsumerStatusBody.class);
        assertEquals(1, body.getConsumerTable().size());
    }
    
    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(defaultTopic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, defaultGroup);
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        result.setKeys("keys");
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setStoreHost(storeHost);
        result.setBornHost(bornHost);
        return result;
    }
}
