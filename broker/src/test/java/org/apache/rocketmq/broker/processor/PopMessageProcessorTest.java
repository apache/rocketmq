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

import com.alibaba.fastjson2.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.broker.processor.PullMessageProcessorTest.createConsumerData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PopMessageProcessorTest {
    private PopMessageProcessor popMessageProcessor;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    private final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
    @Mock
    private DefaultMessageStore messageStore;
    private ClientChannelInfo clientChannelInfo;
    private String group = "FooBarGroup";
    private String topic = "FooBar";

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        brokerController.getBrokerConfig().setEnablePopBufferMerge(true);
        // Initialize BrokerMetricsManager to prevent NPE in tests
        brokerController.setBrokerMetricsManager(new BrokerMetricsManager(brokerController));
        popMessageProcessor = new PopMessageProcessor(brokerController);
        when(handlerContext.channel()).thenReturn(embeddedChannel);
        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig(topic));
        clientChannelInfo = new ClientChannelInfo(embeddedChannel);
        ConsumerData consumerData = createConsumerData(group, topic);
        brokerController.getConsumerManager().registerConsumer(
            consumerData.getGroupName(),
            clientChannelInfo,
            consumerData.getConsumeType(),
            consumerData.getMessageModel(),
            consumerData.getConsumeFromWhere(),
            consumerData.getSubscriptionDataSet(),
            false);
    }

    @Test
    public void testProcessRequest_TopicNotExist() throws RemotingCommandException {
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        brokerController.getTopicConfigManager().getTopicConfigTable().remove(topic);
        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        assertThat(response.getRemark()).contains("topic[" + topic + "] not exist");
    }

    @Test
    public void testProcessRequest_Found() throws RemotingCommandException, InterruptedException {
        GetMessageResult getMessageResult = createGetMessageResult(1);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPopMsgCommand();
        popMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_MsgWasRemoving() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult(1);
        getMessageResult.setStatus(GetMessageStatus.MESSAGE_WAS_REMOVING);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPopMsgCommand();
        popMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_NoMsgInQueue() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult(0);
        getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any())).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNull();
    }

    @Test
    public void testProcessRequest_whenTimerWheelIsFalse() throws RemotingCommandException {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setTimerWheelEnable(false);
        when(messageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        final RemotingCommand request = createPopMsgCommand();
        RemotingCommand response = popMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("pop message is forbidden because timerWheelEnable is false");
    }

    @Test
    public void testGetInitOffset_retryTopic() throws RemotingCommandException {
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        String newGroup = group + "-" + System.currentTimeMillis();
        String retryTopic = KeyBuilder.buildPopRetryTopic(topic, newGroup);
        long minOffset = 100L;
        when(messageStore.getMinOffsetInQueue(retryTopic, 0)).thenReturn(minOffset);
        brokerController.getTopicConfigManager().getTopicConfigTable().put(retryTopic, new TopicConfig(retryTopic, 1, 1));
        GetMessageResult getMessageResult = createGetMessageResult(0);
        when(messageStore.getMessageAsync(eq(newGroup), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(getMessageResult));

        long offset = brokerController.getConsumerOffsetManager().queryOffset(newGroup, retryTopic, 0);
        assertEquals(-1, offset);

        RemotingCommand request = createPopMsgCommand(newGroup, topic, 0, ConsumeInitMode.MAX);
        popMessageProcessor.processRequest(handlerContext, request);
        offset = brokerController.getConsumerOffsetManager().queryOffset(newGroup, retryTopic, 0);
        assertEquals(minOffset, offset);

        when(messageStore.getMinOffsetInQueue(retryTopic, 0)).thenReturn(minOffset * 2);
        popMessageProcessor.processRequest(handlerContext, request);
        offset = brokerController.getConsumerOffsetManager().queryOffset(newGroup, retryTopic, 0);
        assertEquals(minOffset, offset); // will not entry getInitOffset() again
        messageStore.getMinOffsetInQueue(retryTopic, 0); // prevent UnnecessaryStubbingException
    }

    @Test
    public void testGetInitOffset_normalTopic() throws RemotingCommandException, ConsumeQueueException {
        long maxOffset = 999L;
        when(messageStore.getMessageStoreConfig()).thenReturn(new MessageStoreConfig());
        when(messageStore.getMaxOffsetInQueue(topic, 0)).thenReturn(maxOffset);
        String newGroup = group + "-" + System.currentTimeMillis();
        GetMessageResult getMessageResult = createGetMessageResult(0);
        when(messageStore.getMessageAsync(eq(newGroup), anyString(), anyInt(), anyLong(), anyInt(), any()))
                .thenReturn(CompletableFuture.completedFuture(getMessageResult));

        long offset = brokerController.getConsumerOffsetManager().queryOffset(newGroup, topic, 0);
        assertEquals(-1, offset);

        RemotingCommand request = createPopMsgCommand(newGroup, topic, 0, ConsumeInitMode.MAX);
        popMessageProcessor.processRequest(handlerContext, request);
        offset = brokerController.getConsumerOffsetManager().queryOffset(newGroup, topic, 0);
        assertEquals(maxOffset - 1, offset); // checkInMem return false

        when(messageStore.getMaxOffsetInQueue(topic, 0)).thenReturn(maxOffset * 2);
        popMessageProcessor.processRequest(handlerContext, request);
        offset = brokerController.getConsumerOffsetManager().queryOffset(newGroup, topic, 0);
        assertEquals(maxOffset - 1, offset); // will not entry getInitOffset() again
        messageStore.getMaxOffsetInQueue(topic, 0); // prevent UnnecessaryStubbingException
    }

    @Test
    public void testBuildCkMsgJsonParsing() {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setTopic("TestTopic");
        ck.setQueueId(1);
        ck.setStartOffset(100L);
        ck.setCId("TestConsumer");
        ck.setPopTime(System.currentTimeMillis());
        ck.setBrokerName("TestBroker");

        int reviveQid = 0;
        PopMessageProcessor processor = new PopMessageProcessor(brokerController);

        MessageExtBrokerInner result = processor.buildCkMsg(ck, reviveQid);

        String jsonBody = new String(result.getBody(), StandardCharsets.UTF_8);
        PopCheckPoint actual = JSON.parseObject(jsonBody, PopCheckPoint.class);

        assertEquals(ck.getTopic(), actual.getTopic());
        assertEquals(ck.getQueueId(), actual.getQueueId());
        assertEquals(ck.getStartOffset(), actual.getStartOffset());
        assertEquals(ck.getCId(), actual.getCId());
        assertEquals(ck.getPopTime(), actual.getPopTime());
        assertEquals(ck.getBrokerName(), actual.getBrokerName());
        assertEquals(ck.getReviveTime(), actual.getReviveTime());
    }

    private RemotingCommand createPopMsgCommand() {
        return createPopMsgCommand(group, topic, -1, ConsumeInitMode.MAX);
    }

    private RemotingCommand createPopMsgCommand(String group, String topic, int queueId, int initMode) {
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setMaxMsgNums(30);
        requestHeader.setQueueId(queueId);
        requestHeader.setTopic(topic);
        requestHeader.setInvisibleTime(10_000);
        requestHeader.setInitMode(initMode);
        requestHeader.setOrder(false);
        requestHeader.setPollTime(15_000);
        requestHeader.setBornTime(System.currentTimeMillis());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private GetMessageResult createGetMessageResult(int msgCnt) {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);
        getMessageResult.setMinOffset(100);
        getMessageResult.setMaxOffset(1024);
        getMessageResult.setNextBeginOffset(516);
        for (int i = 0; i < msgCnt; i++) {
            ByteBuffer bb = ByteBuffer.allocate(64);
            bb.putLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION, System.currentTimeMillis());
            getMessageResult.addMessage(new SelectMappedBufferResult(200, bb, 64, new DefaultMappedFile()));
        }
        return getMessageResult;
    }
}
