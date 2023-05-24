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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PullMessageProcessorTest {
    private PullMessageProcessor pullMessageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    private final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
    @Mock
    private MessageStore messageStore;
    private ClientChannelInfo clientChannelInfo;
    private String group = "FooBarGroup";
    private String topic = "FooBar";

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager(brokerController);
        pullMessageProcessor = new PullMessageProcessor(brokerController);
        when(brokerController.getPullMessageProcessor()).thenReturn(pullMessageProcessor);
        when(handlerContext.channel()).thenReturn(embeddedChannel);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig());
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
        brokerController.getTopicConfigManager().getTopicConfigTable().remove(topic);
        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        RemotingCommand response = pullMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        assertThat(response.getRemark()).contains("topic[" + topic + "] not exist");
    }

    @Test
    public void testProcessRequest_SubNotExist() throws RemotingCommandException {
        brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, false);
        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        RemotingCommand response = pullMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_NOT_EXIST);
        assertThat(response.getRemark()).contains("consumer's group info not exist");
    }

    @Test
    public void testProcessRequest_SubNotLatest() throws RemotingCommandException {
        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        request.addExtField("subVersion", String.valueOf(101));
        RemotingCommand response = pullMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_NOT_LATEST);
        assertThat(response.getRemark()).contains("subscription not latest");
    }

    @Test
    public void testProcessRequest_Found() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult();
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any(ExpressionMessageFilter.class))).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        pullMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_FoundWithHook() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult();
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any(ExpressionMessageFilter.class))).thenReturn(CompletableFuture.completedFuture(getMessageResult));
        List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
        final ConsumeMessageContext[] messageContext = new ConsumeMessageContext[1];
        ConsumeMessageHook consumeMessageHook = new ConsumeMessageHook() {
            @Override
            public String hookName() {
                return "TestHook";
            }

            @Override
            public void consumeMessageBefore(ConsumeMessageContext context) {
                messageContext[0] = context;
            }

            @Override
            public void consumeMessageAfter(ConsumeMessageContext context) {
            }
        };
        consumeMessageHookList.add(consumeMessageHook);
        pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);
        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        pullMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(messageContext[0]).isNotNull();
        assertThat(messageContext[0].getConsumerGroup()).isEqualTo(group);
        assertThat(messageContext[0].getTopic()).isEqualTo(topic);
        assertThat(messageContext[0].getQueueId()).isEqualTo(1);
    }

    @Test
    public void testProcessRequest_MsgWasRemoving() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.MESSAGE_WAS_REMOVING);
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any(ExpressionMessageFilter.class))).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        pullMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.PULL_RETRY_IMMEDIATELY);
    }

    @Test
    public void testProcessRequest_NoMsgInQueue() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any(ExpressionMessageFilter.class))).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        pullMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.PULL_OFFSET_MOVED);
    }

    @Test
    public void test_LitePullRequestForbidden() throws Exception {
        brokerController.getBrokerConfig().setLitePullMessageEnable(false);
        RemotingCommand remotingCommand = createPullMsgCommand(RequestCode.LITE_PULL_MESSAGE);
        RemotingCommand response = pullMessageProcessor.processRequest(handlerContext, remotingCommand);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testIfBroadcast() throws Exception {
        Class<? extends PullMessageProcessor> clazz = pullMessageProcessor.getClass();
        Method method = clazz.getDeclaredMethod("isBroadcast", boolean.class, ConsumerGroupInfo.class);
        method.setAccessible(true);

        ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo("GID-1",
            ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        Assert.assertTrue((Boolean) method.invoke(pullMessageProcessor, true, consumerGroupInfo));

        ConsumerGroupInfo consumerGroupInfo2 = new ConsumerGroupInfo("GID-2",
            ConsumeType.CONSUME_ACTIVELY, MessageModel.BROADCASTING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        Assert.assertFalse((Boolean) method.invoke(pullMessageProcessor, false, consumerGroupInfo2));

        ConsumerGroupInfo consumerGroupInfo3 = new ConsumerGroupInfo("GID-3",
            ConsumeType.CONSUME_PASSIVELY, MessageModel.BROADCASTING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        Assert.assertTrue((Boolean) method.invoke(pullMessageProcessor, false, consumerGroupInfo3));
    }

    @Test
    public void testCommitPullOffset() throws RemotingCommandException {
        GetMessageResult getMessageResult = createGetMessageResult();
        when(messageStore.getMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(), any(ExpressionMessageFilter.class))).thenReturn(CompletableFuture.completedFuture(getMessageResult));

        final RemotingCommand request = createPullMsgCommand(RequestCode.PULL_MESSAGE);
        pullMessageProcessor.processRequest(handlerContext, request);
        RemotingCommand response = embeddedChannel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(this.brokerController.getConsumerOffsetManager().queryPullOffset(group, topic, 1))
            .isEqualTo(getMessageResult.getNextBeginOffset());
    }

    private RemotingCommand createPullMsgCommand(int requestCode) {
        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setCommitOffset(123L);
        requestHeader.setConsumerGroup(group);
        requestHeader.setMaxMsgNums(100);
        requestHeader.setQueueId(1);
        requestHeader.setQueueOffset(456L);
        requestHeader.setSubscription("*");
        requestHeader.setTopic(topic);
        requestHeader.setSysFlag(0);
        requestHeader.setSubVersion(100L);
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    static ConsumerData createConsumerData(String group, String topic) {
        ConsumerData consumerData = new ConsumerData();
        consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumerData.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerData.setGroupName(group);
        consumerData.setMessageModel(MessageModel.CLUSTERING);
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString("*");
        subscriptionData.setSubVersion(100L);
        subscriptionDataSet.add(subscriptionData);
        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        return consumerData;
    }

    private GetMessageResult createGetMessageResult() {
        GetMessageResult getMessageResult = new GetMessageResult();
        getMessageResult.setStatus(GetMessageStatus.FOUND);
        getMessageResult.setMinOffset(100);
        getMessageResult.setMaxOffset(1024);
        getMessageResult.setNextBeginOffset(516);
        return getMessageResult;
    }
}
