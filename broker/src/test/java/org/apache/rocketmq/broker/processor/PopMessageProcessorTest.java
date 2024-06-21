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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.message.MessageDecoder;
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
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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

    private RemotingCommand createPopMsgCommand() {
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(group);
        requestHeader.setMaxMsgNums(30);
        requestHeader.setQueueId(-1);
        requestHeader.setTopic(topic);
        requestHeader.setInvisibleTime(10_000);
        requestHeader.setInitMode(ConsumeInitMode.MAX);
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
