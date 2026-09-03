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
package org.apache.rocketmq.auth.authorization.builder;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.TelemetryCommand;
import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.common.lite.LiteSubscriptionAction;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestHeaderRegistry;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.body.CreateTopicListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.DeleteSubscriptionGroupListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.DeleteTopicListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LiteSubscriptionCtlRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupList;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteClientInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteGroupInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetParentTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.HeartbeatRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.protocol.header.TriggerLiteDispatchRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultAuthorizationContextBuilderTest {

    private AuthorizationContextBuilder builder;

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Mock
    private Channel channel;

    @Before
    public void setUp() throws Exception {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setClusterName("DefaultCluster");
        builder = new DefaultAuthorizationContextBuilder(authConfig);
        RequestHeaderRegistry.getInstance().initialize();
    }

    @Test
    public void buildGrpc() {
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "rocketmq");
        metadata.put(GrpcConstants.REMOTE_ADDRESS, "192.168.0.1");
        metadata.put(GrpcConstants.CHANNEL_ID, "channel-id");

        GeneratedMessageV3 request = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setTopic(Resource.newBuilder().setName("topic").build())
                .build())
            .build();
        List<DefaultAuthorizationContext> result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(result.get(0).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));
        Assert.assertEquals(result.get(0).getSourceIp(), "192.168.0.1");
        Assert.assertEquals(result.get(0).getChannelId(), "channel-id");
        Assert.assertEquals(result.get(0).getRpcCode(), SendMessageRequest.getDescriptor().getFullName());

        request = RecallMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setRecallHandle("handle")
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(result.get(0).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));
        Assert.assertEquals(result.get(0).getSourceIp(), "192.168.0.1");
        Assert.assertEquals(result.get(0).getChannelId(), "channel-id");
        Assert.assertEquals(result.get(0).getRpcCode(), RecallMessageRequest.getDescriptor().getFullName());

        request = EndTransactionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(result.get(0).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));

        request = HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(result.get(0).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = ReceiveMessageRequest.newBuilder()
            .setMessageQueue(MessageQueue.newBuilder()
                .setTopic(Resource.newBuilder().setName("topic").build())
                .build())
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = AckMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = ForwardMessageToDeadLetterQueueRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = NotifyClientTerminationRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(result.get(0).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = ChangeInvisibleDurationRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("group").build())
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setLiteTopic("liteTopic")
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = QueryRouteRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(result.get(0).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB, Action.SUB)));

        request = QueryAssignmentRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("topic").build())
            .setGroup(Resource.newBuilder().setName("group").build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        request = TelemetryCommand.newBuilder()
            .setSettings(Settings.newBuilder()
                .setPublishing(Publishing.newBuilder()
                    .addTopics(Resource.newBuilder().setName("topic").build())
                    .build())
                .build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.PUB)));

        request = TelemetryCommand.newBuilder()
            .setSettings(Settings.newBuilder()
                .setSubscription(Subscription.newBuilder()
                    .setGroup(Resource.newBuilder().setName("group").build())
                    .addSubscriptions(SubscriptionEntry.newBuilder()
                        .setTopic(Resource.newBuilder().setName("topic").build())
                        .build())
                    .build())
                .build())
            .build();
        result = builder.build(metadata, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.GROUP).getResource().getResourceKey(), "Group:group");
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey(), "User:rocketmq");
        Assert.assertEquals(getContext(result, ResourceType.TOPIC).getResource().getResourceKey(), "Topic:topic");
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));
    }

    @Test
    public void buildRemoting() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);

        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
        sendMessageRequestHeader.setTopic("topic");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, sendMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));
        Assert.assertEquals("192.168.0.1", result.get(0).getSourceIp());
        Assert.assertEquals("channel-id", result.get(0).getChannelId());
        Assert.assertEquals(RequestCode.SEND_MESSAGE + "", result.get(0).getRpcCode());

        sendMessageRequestHeader = new SendMessageRequestHeader();
        sendMessageRequestHeader.setProducerGroup("unrelatedProducer");
        sendMessageRequestHeader.setTopic("%RETRY%group");
        request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, sendMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB)));

        SendMessageRequestHeaderV2 sendMessageRequestHeaderV2 = new SendMessageRequestHeaderV2();
        sendMessageRequestHeaderV2.setTopic("topic");
        request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, sendMessageRequestHeaderV2);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));

        sendMessageRequestHeaderV2 = new SendMessageRequestHeaderV2();
        sendMessageRequestHeaderV2.setA("unrelatedProducer");
        sendMessageRequestHeaderV2.setTopic("%RETRY%group");
        request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, sendMessageRequestHeaderV2);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB)));

        RecallMessageRequestHeader recallMessageRequestHeader = new RecallMessageRequestHeader();
        recallMessageRequestHeader.setTopic("topic");
        recallMessageRequestHeader.setRecallHandle("handle");
        request = RemotingCommand.createRequestCommand(RequestCode.RECALL_MESSAGE, recallMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));
        Assert.assertEquals("192.168.0.1", result.get(0).getSourceIp());
        Assert.assertEquals("channel-id", result.get(0).getChannelId());
        Assert.assertEquals(RequestCode.RECALL_MESSAGE + "", result.get(0).getRpcCode());

        EndTransactionRequestHeader endTransactionRequestHeader = new EndTransactionRequestHeader();
        endTransactionRequestHeader.setTopic("topic");
        request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, endTransactionRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.PUB)));

        endTransactionRequestHeader = new EndTransactionRequestHeader();
        request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, endTransactionRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand endTransactionWithoutTopic = request;
        Assert.assertTrue(builder.build(channelHandlerContext, endTransactionWithoutTopic).isEmpty());
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, consumerSendMsgBackRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB)));

        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topic");
        pullMessageRequestHeader.setConsumerGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.GROUP).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", getContext(result, ResourceType.GROUP).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", getContext(result, ResourceType.TOPIC).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        QueryMessageRequestHeader queryMessageRequestHeader = new QueryMessageRequestHeader();
        queryMessageRequestHeader.setTopic("topic");
        request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, queryMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB, Action.GET)));

        HeartbeatRequestHeader heartbeatRequestHeader = new HeartbeatRequestHeader();
        request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, heartbeatRequestHeader);
        HeartbeatData heartbeatData = new HeartbeatData();
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("group");
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("topic");
        consumerData.setSubscriptionDataSet(Sets.newHashSet(subscriptionData));
        heartbeatData.setConsumerDataSet(Sets.newHashSet(consumerData));
        request.setBody(JSON.toJSONBytes(heartbeatData));
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.GROUP).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", getContext(result, ResourceType.GROUP).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", getContext(result, ResourceType.TOPIC).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        UnregisterClientRequestHeader unregisterClientRequestHeader = new UnregisterClientRequestHeader();
        unregisterClientRequestHeader.setConsumerGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, unregisterClientRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB)));

        GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader = new GetConsumerListByGroupRequestHeader();
        getConsumerListByGroupRequestHeader.setConsumerGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, getConsumerListByGroupRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.SUB, Action.GET)));

        QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader = new QueryConsumerOffsetRequestHeader();
        queryConsumerOffsetRequestHeader.setTopic("topic");
        queryConsumerOffsetRequestHeader.setConsumerGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, queryConsumerOffsetRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.GROUP).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", getContext(result, ResourceType.GROUP).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", getContext(result, ResourceType.TOPIC).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));

        UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader = new UpdateConsumerOffsetRequestHeader();
        updateConsumerOffsetRequestHeader.setTopic("topic");
        updateConsumerOffsetRequestHeader.setConsumerGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, updateConsumerOffsetRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.GROUP).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", getContext(result, ResourceType.GROUP).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB, Action.UPDATE)));
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", getContext(result, ResourceType.TOPIC).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB, Action.UPDATE)));

        CreateTopicRequestHeader createTopicRequestHeader = new CreateTopicRequestHeader();
        createTopicRequestHeader.setTopic("topic");
        createTopicRequestHeader.setTopicFilterType(TopicFilterType.SINGLE_TAG.name());
        request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, createTopicRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.CREATE)));

        CreateUserRequestHeader createUserRequestHeader = new CreateUserRequestHeader();
        createUserRequestHeader.setUsername("abc");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, createUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Cluster:DefaultCluster", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.UPDATE)));

        request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Cluster:DefaultCluster", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.UPDATE)));
        Assert.assertEquals(RequestCode.UPDATE_BROKER_CONFIG + "", result.get(0).getRpcCode());

        request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        result = builder.build(channelHandlerContext, request);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("User:rocketmq", result.get(0).getSubject().getSubjectKey());
        Assert.assertEquals("Cluster:DefaultCluster", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().containsAll(Arrays.asList(Action.GET)));
        Assert.assertEquals(RequestCode.GET_BROKER_CONFIG + "", result.get(0).getRpcCode());
    }

    @Test
    public void rejectBlankResourcesForExistingRemotingRequests() {
        mockRemotingChannel();

        int[] topicCodes = {
            RequestCode.GET_ROUTEINFO_BY_TOPIC,
            RequestCode.SEND_MESSAGE,
            RequestCode.RECALL_MESSAGE,
            RequestCode.QUERY_MESSAGE
        };
        for (int requestCode : topicCodes) {
            RemotingCommand request = remotingRequest(requestCode, null, null);
            request.addExtField("topic", " ");
            Assert.assertThrows(AuthorizationException.class,
                () -> builder.build(channelHandlerContext, request));
        }

        int[] compactTopicCodes = {
            RequestCode.SEND_MESSAGE_V2,
            RequestCode.SEND_BATCH_MESSAGE
        };
        for (int requestCode : compactTopicCodes) {
            RemotingCommand request = remotingRequest(requestCode, null, null);
            request.addExtField("b", " ");
            Assert.assertThrows(AuthorizationException.class,
                () -> builder.build(channelHandlerContext, request));
        }

        ResumeCheckHalfMessageRequestHeader resumeHeader = new ResumeCheckHalfMessageRequestHeader();
        resumeHeader.setMsgId("messageId");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.RESUME_CHECK_HALF_MESSAGE, resumeHeader, null)));
        resumeHeader.setTopic(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.RESUME_CHECK_HALF_MESSAGE, resumeHeader, null)));

        RemotingCommand sendBackRequest = remotingRequest(RequestCode.CONSUMER_SEND_MSG_BACK, null, null);
        sendBackRequest.addExtField("group", " ");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext, sendBackRequest));

        GetConsumerListByGroupRequestHeader listHeader = new GetConsumerListByGroupRequestHeader();
        listHeader.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_CONSUMER_LIST_BY_GROUP, listHeader, null)));

        QueryConsumerOffsetRequestHeader queryOffsetHeader = new QueryConsumerOffsetRequestHeader();
        queryOffsetHeader.setTopic(" ");
        queryOffsetHeader.setConsumerGroup("group");
        queryOffsetHeader.setQueueId(0);
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.QUERY_CONSUMER_OFFSET, queryOffsetHeader, null)));
        queryOffsetHeader.setTopic("topic");
        queryOffsetHeader.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.QUERY_CONSUMER_OFFSET, queryOffsetHeader, null)));

        UpdateConsumerOffsetRequestHeader updateOffsetHeader = new UpdateConsumerOffsetRequestHeader();
        updateOffsetHeader.setTopic(" ");
        updateOffsetHeader.setConsumerGroup("group");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UPDATE_CONSUMER_OFFSET, updateOffsetHeader, null)));
        updateOffsetHeader.setTopic("topic");
        updateOffsetHeader.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UPDATE_CONSUMER_OFFSET, updateOffsetHeader, null)));

        LockBatchRequestBody lockBody = new LockBatchRequestBody();
        lockBody.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LOCK_BATCH_MQ, null, lockBody.encode())));
        lockBody.setConsumerGroup("group");
        lockBody.setMqSet(Collections.singleton(
            new org.apache.rocketmq.common.message.MessageQueue(" ", "broker-a", 0)));
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LOCK_BATCH_MQ, null, lockBody.encode())));

        UnlockBatchRequestBody unlockBody = new UnlockBatchRequestBody();
        unlockBody.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UNLOCK_BATCH_MQ, null, unlockBody.encode())));
        unlockBody.setConsumerGroup("group");
        unlockBody.setMqSet(Collections.singleton(
            new org.apache.rocketmq.common.message.MessageQueue(" ", "broker-a", 0)));
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UNLOCK_BATCH_MQ, null, unlockBody.encode())));

        PopMessageRequestHeader popHeader = new PopMessageRequestHeader();
        popHeader.setConsumerGroup("group");
        popHeader.setTopic(" ");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.POP_MESSAGE, popHeader, null)));
        popHeader.setConsumerGroup(" ");
        popHeader.setTopic("topic");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.POP_MESSAGE, popHeader, null)));
    }

    /**
     * Verifies that annotation-based remoting headers produce the expected authorization contexts
     * after RequestHeaderRegistry initialization.
     */
    @Test
    public void buildRemotingByAnnotation() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);

        PopMessageRequestHeader popMessageRequestHeader = new PopMessageRequestHeader();
        popMessageRequestHeader.setConsumerGroup("group");
        popMessageRequestHeader.setTopic("topic");
        popMessageRequestHeader.setQueueId(0);
        popMessageRequestHeader.setMaxMsgNums(32);
        popMessageRequestHeader.setInvisibleTime(60000L);
        popMessageRequestHeader.setPollTime(15000L);
        popMessageRequestHeader.setBornTime(System.currentTimeMillis());
        popMessageRequestHeader.setInitMode(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, popMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext, request);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.GROUP).getSubject().getSubjectKey());
        Assert.assertEquals("Group:group", getContext(result, ResourceType.GROUP).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.GROUP).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals("User:rocketmq", getContext(result, ResourceType.TOPIC).getSubject().getSubjectKey());
        Assert.assertEquals("Topic:topic", getContext(result, ResourceType.TOPIC).getResource().getResourceKey());
        Assert.assertTrue(getContext(result, ResourceType.TOPIC).getActions().containsAll(Arrays.asList(Action.SUB)));
        Assert.assertEquals("192.168.0.1", getContext(result, ResourceType.TOPIC).getSourceIp());
        Assert.assertEquals(RequestCode.POP_MESSAGE + "", getContext(result, ResourceType.TOPIC).getRpcCode());
    }

    @Test
    public void manuallyResolvedRemotingRequestsAreNotRegisteredForAnnotationFallback() {
        Assert.assertEquals(PopMessageRequestHeader.class, RequestHeaderRegistry.getInstance()
            .getRequestHeader(RequestCode.POP_MESSAGE));
        Assert.assertNull(RequestHeaderRegistry.getInstance()
            .getRequestHeader(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST));
        Assert.assertNull(RequestHeaderRegistry.getInstance()
            .getRequestHeader(RequestCode.GET_ALL_TOPIC_CONFIG));
        Assert.assertNull(RequestHeaderRegistry.getInstance()
            .getRequestHeader(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG));
    }

    @Test
    public void buildRemotingExternalAdminRequests() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);

        int[] readCodes = new int[] {
            RequestCode.GET_BROKER_RUNTIME_INFO,
            RequestCode.GET_ALL_CONSUMER_OFFSET
        };
        for (int requestCode : readCodes) {
            RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, null);
            request.setVersion(441);
            request.addExtField("AccessKey", "rocketmq");
            request.makeCustomHeaderToNet();

            List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext, request);

            Assert.assertEquals(1, result.size());
            Assert.assertEquals("Cluster:DefaultCluster", result.get(0).getResource().getResourceKey());
            Assert.assertTrue(result.get(0).getActions().contains(Action.GET));
            Assert.assertEquals(String.valueOf(requestCode), result.get(0).getRpcCode());
        }
    }

    @Test
    public void buildRemotingUpdateAndCreateSubscriptionGroupRequiresGroupCreate() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);

        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("groupA");
        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.setBody(RemotingSerializable.encode(config));
        request.makeCustomHeaderToNet();

        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext, request);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals("Group:groupA", result.get(0).getResource().getResourceKey());
        Assert.assertTrue(result.get(0).getActions().contains(Action.CREATE));
        Assert.assertEquals(String.valueOf(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP),
            result.get(0).getRpcCode());
    }

    @Test
    public void buildRemotingUpdateAndCreateSubscriptionGroupRejectsMissingGroup() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);

        RemotingCommand request = RemotingCommand.createRequestCommand(
            RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext, request));
    }

    @Test
    public void buildAdditionalRemotingDataRequests() {
        mockRemotingChannel();

        PullMessageRequestHeader litePullHeader = new PullMessageRequestHeader();
        litePullHeader.setTopic("liteTopic");
        litePullHeader.setConsumerGroup("liteGroup");
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LITE_PULL_MESSAGE, litePullHeader, null));
        assertResourceSet(result, "Topic:liteTopic", "Group:liteGroup");
        assertActions(result, "Topic:liteTopic", Action.SUB);
        assertActions(result, "Group:liteGroup", Action.SUB);

        litePullHeader.setTopic("%RETRY%retryGroup");
        litePullHeader.setConsumerGroup("retryGroup");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LITE_PULL_MESSAGE, litePullHeader, null));
        assertResourceOrder(result, "Group:retryGroup");
        assertActions(result, "Group:retryGroup", Action.SUB);

        ViewMessageRequestHeader viewMessageHeader = new ViewMessageRequestHeader();
        viewMessageHeader.setTopic("viewTopic");
        viewMessageHeader.setOffset(0L);
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.VIEW_MESSAGE_BY_ID, viewMessageHeader, null));
        assertResourceOrder(result, "Topic:viewTopic");
        assertActions(result, "Topic:viewTopic", Action.GET);

        viewMessageHeader.setTopic("%RETRY%retryGroup");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.VIEW_MESSAGE_BY_ID, viewMessageHeader, null));
        assertResourceOrder(result, "Group:retryGroup");
        assertActions(result, "Group:retryGroup", Action.GET);

        BatchAck firstAck = batchAck("topicA", "groupA", "0");
        BatchAck duplicateAck = batchAck("topicA", "groupA", "0");
        BatchAck secondAck = batchAck("topicB", "groupB", "0");
        BatchAck retryV1Ack = batchAck("topicA", "groupA", "1");
        BatchAck retryV2Ack = batchAck("topicA", "groupA", "2");
        BatchAckMessageRequestBody batchAckBody = new BatchAckMessageRequestBody();
        batchAckBody.setAcks(Arrays.asList(
            firstAck, duplicateAck, secondAck, retryV1Ack, retryV2Ack));
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.BATCH_ACK_MESSAGE, null, batchAckBody.encode()));
        assertResourceOrder(result,
            "Topic:topicA",
            "Group:groupA",
            "Topic:topicB",
            "Group:groupB");
        for (DefaultAuthorizationContext context : result) {
            Assert.assertEquals(Collections.singletonList(Action.SUB), context.getActions());
        }

        SendMessageRequestHeader replyHeader = new SendMessageRequestHeader();
        replyHeader.setProducerGroup("producerOnly");
        replyHeader.setTopic("replyTopic");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.SEND_REPLY_MESSAGE, replyHeader, null));
        assertResourceOrder(result, "Topic:replyTopic");
        assertActions(result, "Topic:replyTopic", Action.PUB);

        SendMessageRequestHeaderV2 replyHeaderV2 = new SendMessageRequestHeaderV2();
        replyHeaderV2.setA("producerOnly");
        replyHeaderV2.setB("replyTopicV2");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.SEND_REPLY_MESSAGE_V2, replyHeaderV2, null));
        assertResourceOrder(result, "Topic:replyTopicV2");
        assertActions(result, "Topic:replyTopicV2", Action.PUB);

        QueryAssignmentRequestBody assignmentBody = new QueryAssignmentRequestBody();
        assignmentBody.setTopic("assignmentTopic");
        assignmentBody.setConsumerGroup("assignmentGroup");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.QUERY_ASSIGNMENT, null, assignmentBody.encode()));
        assertResourceSet(result, "Topic:assignmentTopic", "Group:assignmentGroup");
        assertActions(result, "Topic:assignmentTopic", Action.SUB);
        assertActions(result, "Group:assignmentGroup", Action.SUB);

        SetMessageRequestModeRequestBody modeBody = new SetMessageRequestModeRequestBody();
        modeBody.setTopic("modeTopic");
        modeBody.setConsumerGroup("modeGroup");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.SET_MESSAGE_REQUEST_MODE, null, modeBody.encode()));
        assertResourceSet(result, "Topic:modeTopic", "Group:modeGroup");
        assertActions(result, "Topic:modeTopic", Action.SUB);
        assertActions(result, "Group:modeGroup", Action.UPDATE);

        CheckClientRequestBody checkClientBody = new CheckClientRequestBody();
        checkClientBody.setGroup("checkGroup");
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("checkTopic");
        checkClientBody.setSubscriptionData(subscriptionData);
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.CHECK_CLIENT_CONFIG, null, checkClientBody.encode()));
        assertResourceSet(result, "Topic:checkTopic", "Group:checkGroup");
        assertActions(result, "Topic:checkTopic", Action.SUB);
        assertActions(result, "Group:checkGroup", Action.SUB);
    }

    @Test
    public void buildConsumerStartOffsetRequestsForTopicReadAndSubscription() {
        mockRemotingChannel();

        GetMaxOffsetRequestHeader header = new GetMaxOffsetRequestHeader();
        header.setTopic("topic");
        header.setQueueId(0);

        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_MAX_OFFSET, header, null));

        assertResourceOrder(result, "Topic:topic");
        assertActions(result, "Topic:topic", Action.SUB, Action.GET);

        header.setTopic("%RETRY%group");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_MAX_OFFSET, header, null));

        assertResourceOrder(result, "Group:group");
        assertActions(result, "Group:group", Action.SUB, Action.GET);

        SearchOffsetRequestHeader searchHeader = new SearchOffsetRequestHeader();
        searchHeader.setTopic("topic");
        searchHeader.setQueueId(0);
        searchHeader.setTimestamp(0L);
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, searchHeader, null));

        assertResourceOrder(result, "Topic:topic");
        assertActions(result, "Topic:topic", Action.SUB, Action.GET);

        searchHeader.setTopic("%RETRY%group");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, searchHeader, null));

        assertResourceOrder(result, "Group:group");
        assertActions(result, "Group:group", Action.SUB, Action.GET);
    }

    @Test
    public void rejectMalformedAdditionalRemotingDataRequests() {
        mockRemotingChannel();

        PullMessageRequestHeader litePullHeader = new PullMessageRequestHeader();
        litePullHeader.setConsumerGroup("liteGroup");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.LITE_PULL_MESSAGE, litePullHeader, null)));
        litePullHeader.setTopic("liteTopic");
        litePullHeader.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.LITE_PULL_MESSAGE, litePullHeader, null)));

        PullMessageRequestHeader mismatchedRetryHeader = new PullMessageRequestHeader();
        mismatchedRetryHeader.setTopic("%RETRY%ownerGroup");
        mismatchedRetryHeader.setConsumerGroup("otherGroup");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.PULL_MESSAGE, mismatchedRetryHeader, null)));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.LITE_PULL_MESSAGE, mismatchedRetryHeader, null)));

        SendMessageRequestHeader replyHeader = new SendMessageRequestHeader();
        replyHeader.setProducerGroup("producerOnly");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.SEND_REPLY_MESSAGE, replyHeader, null)));
        SendMessageRequestHeaderV2 replyHeaderV2 = new SendMessageRequestHeaderV2();
        replyHeaderV2.setA("producerOnly");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.SEND_REPLY_MESSAGE_V2, replyHeaderV2, null)));

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.BATCH_ACK_MESSAGE, null, null)));
        BatchAckMessageRequestBody emptyBatch = new BatchAckMessageRequestBody();
        emptyBatch.setAcks(Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.BATCH_ACK_MESSAGE, null, emptyBatch.encode())));
        BatchAckMessageRequestBody nullEntryBatch = new BatchAckMessageRequestBody();
        nullEntryBatch.setAcks(Collections.singletonList(null));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.BATCH_ACK_MESSAGE, null, nullEntryBatch.encode())));
        BatchAck blankTopicAck = batchAck("topic", "group", "0");
        blankTopicAck.setTopic(" ");
        BatchAckMessageRequestBody blankTopicBatch = new BatchAckMessageRequestBody();
        blankTopicBatch.setAcks(Collections.singletonList(blankTopicAck));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.BATCH_ACK_MESSAGE, null, blankTopicBatch.encode())));
        BatchAck blankGroupAck = batchAck("topic", "group", "0");
        blankGroupAck.setConsumerGroup(" ");
        BatchAckMessageRequestBody blankGroupBatch = new BatchAckMessageRequestBody();
        blankGroupBatch.setAcks(Collections.singletonList(blankGroupAck));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.BATCH_ACK_MESSAGE, null, blankGroupBatch.encode())));

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.QUERY_ASSIGNMENT, null, null)));
        QueryAssignmentRequestBody assignmentBody = new QueryAssignmentRequestBody();
        assignmentBody.setTopic(" ");
        assignmentBody.setConsumerGroup("group");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.QUERY_ASSIGNMENT, null, assignmentBody.encode())));
        assignmentBody.setTopic("topic");
        assignmentBody.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.QUERY_ASSIGNMENT, null, assignmentBody.encode())));

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.SET_MESSAGE_REQUEST_MODE, null, null)));
        SetMessageRequestModeRequestBody modeBody = new SetMessageRequestModeRequestBody();
        modeBody.setTopic(" ");
        modeBody.setConsumerGroup("group");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.SET_MESSAGE_REQUEST_MODE, null, modeBody.encode())));
        modeBody.setTopic("topic");
        modeBody.setConsumerGroup(" ");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.SET_MESSAGE_REQUEST_MODE, null, modeBody.encode())));

        CheckClientRequestBody checkClientBody = new CheckClientRequestBody();
        checkClientBody.setGroup("checkGroup");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.CHECK_CLIENT_CONFIG, null, checkClientBody.encode())));
        SubscriptionData blankSubscription = new SubscriptionData();
        blankSubscription.setTopic(" ");
        checkClientBody.setSubscriptionData(blankSubscription);
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.CHECK_CLIENT_CONFIG, null, checkClientBody.encode())));
        blankSubscription.setTopic("checkTopic");
        checkClientBody.setGroup(" ");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.CHECK_CLIENT_CONFIG, null, checkClientBody.encode())));

        EndTransactionRequestHeader endTransactionHeader = new EndTransactionRequestHeader();
        endTransactionHeader.setProducerGroup("producerOnly");
        RemotingCommand request = remotingRequest(RequestCode.END_TRANSACTION, endTransactionHeader, null);
        Assert.assertTrue(builder.build(channelHandlerContext, request).isEmpty());
        endTransactionHeader.setTopic(" ");
        Assert.assertTrue(builder.build(channelHandlerContext,
            remotingRequest(RequestCode.END_TRANSACTION, endTransactionHeader, null)).isEmpty());

        ViewMessageRequestHeader viewMessageHeader = new ViewMessageRequestHeader();
        viewMessageHeader.setOffset(0L);
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.VIEW_MESSAGE_BY_ID, viewMessageHeader, null)));
        viewMessageHeader.setTopic(" ");
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.VIEW_MESSAGE_BY_ID, viewMessageHeader, null)));
    }

    @Test
    public void buildAdditionalRemotingDataPathBoundaries() {
        mockRemotingChannel();

        SendMessageRequestHeaderV2 batchHeader = new SendMessageRequestHeaderV2();
        batchHeader.setB("%RETRY%batchGroup");
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.SEND_BATCH_MESSAGE, batchHeader, null));
        assertResourceOrder(result, "Group:batchGroup");
        assertActions(result, "Group:batchGroup", Action.SUB);

        PullMessageRequestHeader retryPullHeader = new PullMessageRequestHeader();
        retryPullHeader.setTopic("%RETRY%pullGroup");
        retryPullHeader.setConsumerGroup("pullGroup");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.PULL_MESSAGE, retryPullHeader, null));
        assertResourceOrder(result, "Group:pullGroup");
        assertActions(result, "Group:pullGroup", Action.SUB);

        PullMessageRequestHeader missingPullGroup = new PullMessageRequestHeader();
        missingPullGroup.setTopic("topic");
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.PULL_MESSAGE, missingPullGroup, null)));

        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.HEART_BEAT, new HeartbeatRequestHeader(), null)));

        HeartbeatData heartbeatData = new HeartbeatData();
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("group");
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(" ");
        consumerData.setSubscriptionDataSet(Collections.singleton(subscriptionData));
        heartbeatData.setConsumerDataSet(Collections.singleton(consumerData));
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.HEART_BEAT, new HeartbeatRequestHeader(),
                JSON.toJSONBytes(heartbeatData))));

        RemotingCommand listRequest =
            RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);
        listRequest.setExtFields(null);
        result = builder.build(channelHandlerContext, listRequest);
        assertAnyResource(result, ResourceType.TOPIC);
        assertActions(result, "Topic:*", Action.LIST);
    }

    @Test
    public void buildRemainingResourceAdminRequests() {
        mockRemotingChannel();

        CreateTopicListRequestBody topicListBody = new CreateTopicListRequestBody(Arrays.asList(
            new TopicConfig("topicA"), new TopicConfig("%RETRY%groupA"), new TopicConfig("topicA")));
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, null, topicListBody.encode()));
        assertResourceOrder(result, "Topic:topicA", "Group:groupA");
        assertActions(result, "Topic:topicA", Action.CREATE);
        assertActions(result, "Group:groupA", Action.CREATE);

        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null,
                "groupA=1\ngroupB=2\n".getBytes(StandardCharsets.UTF_8)));
        assertResourceSet(result, "Group:groupA", "Group:groupB");
        assertActions(result, "Group:groupA", Action.UPDATE);
        assertActions(result, "Group:groupB", Action.UPDATE);

        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null,
                "groupA".getBytes(StandardCharsets.UTF_8)));
        assertResourceOrder(result, "Group:groupA");
        assertActions(result, "Group:groupA", Action.UPDATE);

        SubscriptionGroupConfig groupA = new SubscriptionGroupConfig();
        groupA.setGroupName("groupA");
        SubscriptionGroupConfig groupB = new SubscriptionGroupConfig();
        groupB.setGroupName("groupB");
        SubscriptionGroupConfig duplicateGroup = new SubscriptionGroupConfig();
        duplicateGroup.setGroupName("groupA");
        SubscriptionGroupList groupList = new SubscriptionGroupList(Arrays.asList(groupA, groupB, duplicateGroup));
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, null, groupList.encode()));
        assertResourceOrder(result, "Group:groupA", "Group:groupB");
        assertActions(result, "Group:groupA", Action.CREATE);
        assertActions(result, "Group:groupB", Action.CREATE);

        TopicQueueMappingDetail mappingDetail =
            new TopicQueueMappingDetail("staticTopic", 1, "broker-a", 1L);
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC,
                createTopicHeader("staticTopic"), mappingDetail.encode()));
        assertResourceOrder(result, "Topic:staticTopic");
        assertActions(result, "Topic:staticTopic", Action.CREATE);
    }

    @Test
    public void rejectMalformedResourceAdminRequests() {
        mockRemotingChannel();

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, null, null)));
        CreateTopicListRequestBody emptyTopicList =
            new CreateTopicListRequestBody(Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, null, emptyTopicList.encode())));
        CreateTopicListRequestBody nullTopicEntry =
            new CreateTopicListRequestBody(Collections.singletonList(null));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, null, nullTopicEntry.encode())));

        CreateTopicListRequestBody invalidTopicList =
            new CreateTopicListRequestBody(Arrays.asList(new TopicConfig("topicA"), new TopicConfig(" ")));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, null, invalidTopicList.encode())));

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null, new byte[0])));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null, new byte[0])));

        SubscriptionGroupConfig invalidGroup = new SubscriptionGroupConfig();
        invalidGroup.setGroupName(" ");
        SubscriptionGroupList emptyGroupList = new SubscriptionGroupList(Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, null,
                    emptyGroupList.encode())));
        SubscriptionGroupList nullGroupEntry =
            new SubscriptionGroupList(Collections.singletonList(null));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, null,
                    nullGroupEntry.encode())));
        SubscriptionGroupList invalidGroupList =
            new SubscriptionGroupList(Arrays.asList(new SubscriptionGroupConfig(), invalidGroup));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, null,
                    invalidGroupList.encode())));

        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC, createTopicHeader(" "), null)));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC,
                    createTopicHeader("staticTopic"), null)));
        TopicQueueMappingDetail blankMapping =
            new TopicQueueMappingDetail(" ", 1, "broker-a", 1L);
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC,
                    createTopicHeader("staticTopic"), blankMapping.encode())));
        TopicQueueMappingDetail mismatchedMapping =
            new TopicQueueMappingDetail("otherTopic", 1, "broker-a", 1L);
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(channelHandlerContext,
                remotingRequest(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC,
                    createTopicHeader("staticTopic"), mismatchedMapping.encode())));
    }

    @Test
    public void buildRemainingAdminReadRequests() {
        mockRemotingChannel();

        int[] topicListCodes = new int[] {
            RequestCode.GET_ALL_TOPIC_CONFIG,
            RequestCode.GET_TIMER_METRICS,
            RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER
        };
        for (int requestCode : topicListCodes) {
            List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
                remotingRequest(requestCode, null, null));
            assertResourceOrder(result, "Topic:*");
            assertAnyResource(result, ResourceType.TOPIC);
            assertActions(result, "Topic:*", Action.LIST);
        }

        int[] groupListCodes = new int[] {
            RequestCode.GET_COLD_DATA_FLOW_CTR_INFO,
            RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG
        };
        for (int requestCode : groupListCodes) {
            List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
                remotingRequest(requestCode, null, null));
            assertResourceOrder(result, "Group:*");
            assertAnyResource(result, ResourceType.GROUP);
            assertActions(result, "Group:*", Action.LIST);
        }

        List<DefaultAuthorizationContext> requestModeResult = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_ALL_MESSAGE_REQUEST_MODE, null, null));
        assertResourceOrder(requestModeResult, "Topic:*", "Group:*");
        assertAnyResource(requestModeResult, ResourceType.TOPIC);
        assertAnyResource(requestModeResult, ResourceType.GROUP);
        assertActions(requestModeResult, "Topic:*", Action.LIST);
        assertActions(requestModeResult, "Group:*", Action.LIST);

        int[] clusterGetCodes = new int[] {
            RequestCode.GET_TIMER_CHECK_POINT,
            RequestCode.GET_ALL_DELAY_OFFSET,
            RequestCode.GET_BROKER_HA_STATUS,
            RequestCode.GET_BROKER_EPOCH_CACHE,
            RequestCode.GET_BROKER_LITE_INFO
        };
        for (int requestCode : clusterGetCodes) {
            List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
                remotingRequest(requestCode, null, null));
            assertResourceOrder(result, "Cluster:DefaultCluster");
            assertActions(result, "Cluster:DefaultCluster", Action.GET);
        }

        int[] updateCodes = new int[] {
            RequestCode.SET_COMMITLOG_READ_MODE,
            RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE,
            RequestCode.DELETE_EXPIRED_COMMITLOG,
            RequestCode.CLEAN_UNUSED_TOPIC,
            RequestCode.POP_ROLLBACK,
            RequestCode.SWITCH_TIMER_ENGINE
        };
        for (int requestCode : updateCodes) {
            List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
                remotingRequest(requestCode, null, null));
            assertResourceOrder(result, "Cluster:DefaultCluster");
            assertActions(result, "Cluster:DefaultCluster", Action.UPDATE);
        }
    }

    @Test
    public void buildLiteHeaderRequests() {
        mockRemotingChannel();

        PopLiteMessageRequestHeader popHeader = new PopLiteMessageRequestHeader();
        popHeader.setClientId("clientA");
        popHeader.setConsumerGroup("groupA");
        popHeader.setTopic("topicA");
        popHeader.setMaxMsgNum(16);
        popHeader.setInvisibleTime(3000);
        popHeader.setPollTime(1000);
        popHeader.setBornTime(System.currentTimeMillis());
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.POP_LITE_MESSAGE, popHeader, null));
        assertResourceOrder(result, "Group:groupA", "Topic:topicA");
        assertActions(result, "Group:groupA", Action.SUB);
        assertActions(result, "Topic:topicA", Action.SUB);

        GetParentTopicInfoRequestHeader parentTopicHeader = new GetParentTopicInfoRequestHeader();
        parentTopicHeader.setTopic("topicA");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_PARENT_TOPIC_INFO, parentTopicHeader, null));
        assertResourceOrder(result, "Topic:topicA");
        assertActions(result, "Topic:topicA", Action.GET);

        GetLiteTopicInfoRequestHeader liteTopicHeader = new GetLiteTopicInfoRequestHeader();
        liteTopicHeader.setParentTopic("topicA");
        liteTopicHeader.setLiteTopic("liteTopicA");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_LITE_TOPIC_INFO, liteTopicHeader, null));
        assertResourceOrder(result, "Topic:topicA");
        assertActions(result, "Topic:topicA", Action.GET);

        GetLiteClientInfoRequestHeader clientInfoHeader = new GetLiteClientInfoRequestHeader();
        clientInfoHeader.setParentTopic("topicA");
        clientInfoHeader.setGroup("groupA");
        clientInfoHeader.setClientId("clientA");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_LITE_CLIENT_INFO, clientInfoHeader, null));
        assertResourceOrder(result, "Topic:topicA", "Group:groupA");
        assertActions(result, "Topic:topicA", Action.GET);
        assertActions(result, "Group:groupA", Action.GET);

        GetLiteGroupInfoRequestHeader groupInfoHeader = new GetLiteGroupInfoRequestHeader();
        groupInfoHeader.setGroup("groupA");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.GET_LITE_GROUP_INFO, groupInfoHeader, null));
        assertResourceOrder(result, "Group:groupA");
        assertActions(result, "Group:groupA", Action.GET);

        TriggerLiteDispatchRequestHeader dispatchHeader = new TriggerLiteDispatchRequestHeader();
        dispatchHeader.setGroup("groupA");
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.TRIGGER_LITE_DISPATCH, dispatchHeader, null));
        assertResourceOrder(result, "Group:groupA");
        assertActions(result, "Group:groupA", Action.UPDATE);
    }

    @Test
    public void buildLiteSubscriptionControlFromBody() {
        mockRemotingChannel();

        LiteSubscriptionDTO subscriptionA = new LiteSubscriptionDTO()
            .setAction(LiteSubscriptionAction.PARTIAL_ADD)
            .setClientId("clientA")
            .setGroup("groupA")
            .setTopic("topicA");
        LiteSubscriptionDTO subscriptionB = new LiteSubscriptionDTO()
            .setAction(LiteSubscriptionAction.COMPLETE_ADD)
            .setClientId("clientB")
            .setGroup("groupB")
            .setTopic("topicA");
        LiteSubscriptionCtlRequestBody requestBody = new LiteSubscriptionCtlRequestBody();
        requestBody.setSubscriptionSet(new LinkedHashSet<>(Arrays.asList(subscriptionA, subscriptionB)));

        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LITE_SUBSCRIPTION_CTL, null, requestBody.encode()));
        assertResourceSet(result, "Group:groupA", "Topic:topicA", "Group:groupB");
        assertActions(result, "Group:groupA", Action.SUB);
        assertActions(result, "Topic:topicA", Action.SUB);
        assertActions(result, "Group:groupB", Action.SUB);
    }

    @Test
    public void buildBatchDeleteRequests() {
        mockRemotingChannel();

        DeleteTopicListRequestBody topicListBody = new DeleteTopicListRequestBody();
        topicListBody.setTopicList(Arrays.asList("topicA", "%RETRY%groupA", "topicA"));
        List<DefaultAuthorizationContext> result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.DELETE_TOPIC_IN_BROKER_LIST, null, topicListBody.encode()));
        assertResourceOrder(result, "Topic:topicA", "Group:groupA");
        assertActions(result, "Topic:topicA", Action.DELETE);
        assertActions(result, "Group:groupA", Action.DELETE);
        for (DefaultAuthorizationContext context : result) {
            Assert.assertEquals(String.valueOf(RequestCode.DELETE_TOPIC_IN_BROKER_LIST), context.getRpcCode());
        }

        DeleteSubscriptionGroupListRequestBody groupListBody = new DeleteSubscriptionGroupListRequestBody();
        groupListBody.setGroupNameList(Arrays.asList("groupX", "groupY"));
        result = builder.build(channelHandlerContext,
            remotingRequest(RequestCode.DELETE_SUBSCRIPTION_GROUP_LIST, null, groupListBody.encode()));
        assertResourceOrder(result, "Group:groupX", "Group:groupY");
        assertActions(result, "Group:groupX", Action.DELETE);
        assertActions(result, "Group:groupY", Action.DELETE);
        for (DefaultAuthorizationContext context : result) {
            Assert.assertEquals(String.valueOf(RequestCode.DELETE_SUBSCRIPTION_GROUP_LIST), context.getRpcCode());
        }
    }

    @Test
    public void rejectMalformedBodyDrivenRequests() {
        mockRemotingChannel();

        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LITE_SUBSCRIPTION_CTL, null, null)));

        LiteSubscriptionCtlRequestBody emptySubscriptionBody = new LiteSubscriptionCtlRequestBody();
        emptySubscriptionBody.setSubscriptionSet(Collections.emptySet());
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LITE_SUBSCRIPTION_CTL, null, emptySubscriptionBody.encode())));

        LiteSubscriptionDTO invalidSubscription = new LiteSubscriptionDTO()
            .setAction(LiteSubscriptionAction.PARTIAL_ADD)
            .setClientId("clientA")
            .setGroup(" ")
            .setTopic("topicA");
        LiteSubscriptionCtlRequestBody invalidSubscriptionBody = new LiteSubscriptionCtlRequestBody();
        invalidSubscriptionBody.setSubscriptionSet(Collections.singleton(invalidSubscription));
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.LITE_SUBSCRIPTION_CTL, null, invalidSubscriptionBody.encode())));

        DeleteTopicListRequestBody emptyTopicList = new DeleteTopicListRequestBody(Collections.emptyList());
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.DELETE_TOPIC_IN_BROKER_LIST, null, emptyTopicList.encode())));

        DeleteTopicListRequestBody invalidTopicList = new DeleteTopicListRequestBody(
            Arrays.asList("topicA", " "));
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.DELETE_TOPIC_IN_BROKER_LIST, null, invalidTopicList.encode())));

        DeleteSubscriptionGroupListRequestBody invalidGroupList =
            new DeleteSubscriptionGroupListRequestBody(Arrays.asList("groupA", " "));
        Assert.assertThrows(AuthorizationException.class, () -> builder.build(channelHandlerContext,
            remotingRequest(RequestCode.DELETE_SUBSCRIPTION_GROUP_LIST, null, invalidGroupList.encode())));
    }

    @Test
    public void buildGrpcHeartbeatByClientShape() {
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "rocketmq");
        metadata.put(GrpcConstants.REMOTE_ADDRESS, "192.168.0.1");
        metadata.put(GrpcConstants.CHANNEL_ID, "channel-id");

        List<DefaultAuthorizationContext> result = builder.build(metadata, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .setGroup(Resource.newBuilder().setName("historicalConsumer"))
            .build());
        assertResourceOrder(result, "Group:historicalConsumer");
        assertActions(result, "Group:historicalConsumer", Action.SUB);

        Assert.assertNull(builder.build(metadata, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build()));
        Assert.assertNull(builder.build(metadata, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .build()));
        Assert.assertThrows(AuthorizationException.class,
            () -> builder.build(metadata, HeartbeatRequest.newBuilder()
                .setClientType(ClientType.PRODUCER)
                .setGroup(Resource.newBuilder().setName("mustNotBecomeGroup"))
                .build()));
    }

    @Test
    public void buildGrpcLiteSubscriptionIgnoresLiteTopics() {
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "rocketmq");
        metadata.put(GrpcConstants.REMOTE_ADDRESS, "192.168.0.1");
        metadata.put(GrpcConstants.CHANNEL_ID, "channel-id");

        SyncLiteSubscriptionRequest emptyLiteTopics = SyncLiteSubscriptionRequest.newBuilder()
            .setAction(apache.rocketmq.v2.LiteSubscriptionAction.COMPLETE_REMOVE)
            .setTopic(Resource.newBuilder().setName("parentTopic"))
            .setGroup(Resource.newBuilder().setName("group"))
            .build();
        List<DefaultAuthorizationContext> result = builder.build(metadata, emptyLiteTopics);
        assertResourceOrder(result, "Group:group", "Topic:parentTopic");
        assertActions(result, "Group:group", Action.SUB);
        assertActions(result, "Topic:parentTopic", Action.SUB);

        SyncLiteSubscriptionRequest withLiteTopics = SyncLiteSubscriptionRequest.newBuilder()
            .setAction(apache.rocketmq.v2.LiteSubscriptionAction.PARTIAL_ADD)
            .setTopic(Resource.newBuilder().setName("parentTopic"))
            .setGroup(Resource.newBuilder().setName("group"))
            .addLiteTopicSet("liteTopic")
            .build();
        result = builder.build(metadata, withLiteTopics);
        assertResourceOrder(result, "Group:group", "Topic:parentTopic");
        assertActions(result, "Group:group", Action.SUB);
        assertActions(result, "Topic:parentTopic", Action.SUB);
    }

    private BatchAck batchAck(String topic, String group, String retry) {
        BatchAck ack = new BatchAck();
        ack.setTopic(topic);
        ack.setConsumerGroup(group);
        ack.setRetry(retry);
        BitSet bitSet = new BitSet();
        bitSet.set(0);
        ack.setBitSet(bitSet);
        return ack;
    }

    private RemotingCommand remotingRequest(int requestCode, CommandCustomHeader header, byte[] body) {
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, header);
        request.addExtField("AccessKey", "rocketmq");
        request.setBody(body);
        request.makeCustomHeaderToNet();
        return request;
    }

    private CreateTopicRequestHeader createTopicHeader(String topic) {
        CreateTopicRequestHeader header = new CreateTopicRequestHeader();
        header.setTopic(topic);
        header.setDefaultTopic("defaultTopic");
        header.setReadQueueNums(8);
        header.setWriteQueueNums(8);
        header.setPerm(6);
        header.setTopicFilterType(TopicFilterType.SINGLE_TAG.name());
        header.setOrder(false);
        return header;
    }

    private void mockRemotingChannel() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);
    }

    private void assertResourceOrder(List<DefaultAuthorizationContext> contexts, String... resourceKeys) {
        Assert.assertEquals(resourceKeys.length, contexts.size());
        for (int i = 0; i < resourceKeys.length; i++) {
            Assert.assertEquals(resourceKeys[i], contexts.get(i).getResource().getResourceKey());
        }
    }

    private void assertResourceSet(List<DefaultAuthorizationContext> contexts, String... resourceKeys) {
        Set<String> actual = new LinkedHashSet<>();
        for (DefaultAuthorizationContext context : contexts) {
            actual.add(context.getResource().getResourceKey());
        }
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(resourceKeys)), actual);
        Assert.assertEquals(resourceKeys.length, contexts.size());
    }

    private void assertActions(List<DefaultAuthorizationContext> contexts, String resourceKey, Action... actions) {
        DefaultAuthorizationContext context = contexts.stream()
            .filter(item -> resourceKey.equals(item.getResource().getResourceKey()))
            .findFirst()
            .orElse(null);
        Assert.assertNotNull(context);
        Assert.assertEquals(new LinkedHashSet<>(Arrays.asList(actions)),
            new LinkedHashSet<>(context.getActions()));
    }

    private DefaultAuthorizationContext getContext(List<DefaultAuthorizationContext> contexts,
        ResourceType resourceType) {
        return contexts.stream().filter(context -> context.getResource().getResourceType() == resourceType)
            .findFirst().orElse(null);
    }

    private void assertAnyResource(List<DefaultAuthorizationContext> contexts, ResourceType resourceType) {
        DefaultAuthorizationContext context = getContext(contexts, resourceType);
        Assert.assertNotNull(context);
        Assert.assertEquals(ResourcePattern.ANY, context.getResource().getResourcePattern());
        Assert.assertNull(context.getResource().getResourceName());
    }

    private ChannelId mockChannelId(String channelId) {
        return new ChannelId() {
            @Override
            public String asShortText() {
                return channelId;
            }

            @Override
            public String asLongText() {
                return channelId;
            }

            @Override
            public int compareTo(ChannelId o) {
                return 0;
            }
        };
    }

    private Attribute<String> mockAttribute(String value) {
        return new Attribute<String>() {
            @Override
            public AttributeKey<String> key() {
                return null;
            }

            @Override
            public String get() {
                return value;
            }

            @Override
            public void set(String value) {
            }

            @Override
            public String getAndSet(String value) {
                return null;
            }

            @Override
            public String setIfAbsent(String value) {
                return null;
            }

            @Override
            public String getAndRemove() {
                return null;
            }

            @Override
            public boolean compareAndSet(String oldValue, String newValue) {
                return false;
            }

            @Override
            public void remove() {

            }
        };
    }
}
