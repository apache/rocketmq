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
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.PopBufferMergeService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.broker.processor.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ChangeInvisibleTimeProcessorTest {
    private ChangeInvisibleTimeProcessor changeInvisibleTimeProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private DefaultMessageStore messageStore;
    @Mock
    private Channel channel;

    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ClientChannelInfo clientInfo;
    @Mock
    private Broker2Client broker2Client;

    @Mock
    private EscapeBridge escapeBridge;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        // Inject BrokerMetricsManager if missing
        Field brokerMetricsManagerField = BrokerController.class.getDeclaredField("brokerMetricsManager");
        brokerMetricsManagerField.setAccessible(true);
        if (brokerMetricsManagerField.get(brokerController) == null) {
            BrokerMetricsManager brokerMetricsManager = new BrokerMetricsManager(brokerController);
            brokerMetricsManagerField.set(brokerController, brokerMetricsManager);
        }
        
        // Mock necessary dependencies
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getEscapeBridge()).thenReturn(this.escapeBridge);
        
        Channel mockChannel = mock(Channel.class);
        when(handlerContext.channel()).thenReturn(mockChannel);
        
        // Mock TopicConfigManager
        TopicConfigManager topicConfigManager = mock(TopicConfigManager.class);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfigTable.put(topic, topicConfig);
        when(topicConfigManager.selectTopicConfig(topic)).thenReturn(topicConfig);
        
        // Mock BrokerStatsManager
        BrokerStatsManager brokerStatsManager = mock(BrokerStatsManager.class);
        when(brokerController.getBrokerStatsManager()).thenReturn(brokerStatsManager);
        
        // Mock PopMessageProcessor and PopBufferMergeService
        PopMessageProcessor popMessageProcessor = mock(PopMessageProcessor.class);
        PopBufferMergeService popBufferMergeService = mock(PopBufferMergeService.class);
        when(brokerController.getPopMessageProcessor()).thenReturn(popMessageProcessor);
        when(popMessageProcessor.getPopBufferMergeService()).thenReturn(popBufferMergeService);
        
        clientInfo = new ClientChannelInfo(channel, "127.0.0.1", LanguageCode.JAVA, 0);
        changeInvisibleTimeProcessor = new ChangeInvisibleTimeProcessor(brokerController);
    }

    @Test
    public void testProcessRequest_Success() throws RemotingCommandException, ConsumeQueueException {
        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(2L);
        when(escapeBridge.asyncPutMessageToSpecificQueue(any(MessageExtBrokerInner.class))).thenReturn(CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK))));
        int queueId = 0;
        long queueOffset = 0;
        long popTime = System.currentTimeMillis() - 1_000;
        long invisibleTime = 30_000;
        int reviveQid = 0;
        String brokerName = "test_broker";
        String extraInfo = ExtraInfoUtil.buildExtraInfo(queueOffset, popTime, invisibleTime, reviveQid,
            topic, brokerName, queueId) + MessageConst.KEY_SEPARATOR + queueOffset;

        ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setOffset(queueOffset);
        requestHeader.setConsumerGroup(group);
        requestHeader.setExtraInfo(extraInfo);
        requestHeader.setInvisibleTime(invisibleTime);

        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand responseToReturn = changeInvisibleTimeProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(responseToReturn.getOpaque()).isEqualTo(request.getOpaque());
    }

    @Test
    public void testProcessRequest_NoMessage() throws RemotingCommandException, ConsumeQueueException {
        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(2L);
        int queueId = 0;
        long queueOffset = 2;
        long popTime = System.currentTimeMillis() - 1_000;
        long invisibleTime = 30_000;
        int reviveQid = 0;
        String brokerName = "test_broker";
        String extraInfo = ExtraInfoUtil.buildExtraInfo(queueOffset, popTime, invisibleTime, reviveQid,
            topic, brokerName, queueId) + MessageConst.KEY_SEPARATOR + queueOffset;

        ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setOffset(queueOffset);
        requestHeader.setConsumerGroup(group);
        requestHeader.setExtraInfo(extraInfo);
        requestHeader.setInvisibleTime(invisibleTime);

        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand responseToReturn = changeInvisibleTimeProcessor.processRequest(handlerContext, request);
        assertThat(responseToReturn.getCode()).isEqualTo(ResponseCode.NO_MESSAGE);
        assertThat(responseToReturn.getOpaque()).isEqualTo(request.getOpaque());
    }
}
