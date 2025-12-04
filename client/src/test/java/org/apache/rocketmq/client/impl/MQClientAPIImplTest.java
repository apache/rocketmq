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
package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ObjectCreator;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.EpochEntryCache;
import org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerInfo;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MQClientAPIImplTest {

    private MQClientAPIImpl mqClientAPI = new MQClientAPIImpl(new NettyClientConfig(), null, null, new ClientConfig());

    @Mock
    private RemotingClient remotingClient;

    @Mock
    private DefaultMQProducerImpl defaultMQProducerImpl;

    @Mock
    private RemotingCommand response;

    private final String brokerAddr = "127.0.0.1";

    private final String brokerName = "DefaultBroker";

    private final String clusterName = "DefaultCluster";

    private final String group = "FooBarGroup";

    private final String topic = "FooBar";

    private final Message msg = new Message("FooBar", new byte[]{});

    private final String clientId = "127.0.0.2@UnitTest";

    private final String defaultTopic = "defaultTopic";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final String defaultNsAddr = "127.0.0.1:9876";

    private final long defaultTimeout = 3000L;

    @Before
    public void init() throws Exception {
        Field field = MQClientAPIImpl.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(mqClientAPI, remotingClient);
    }

    @Test
    public void testSendMessageOneWay_Success() throws RemotingException, InterruptedException, MQBrokerException {
        doNothing().when(remotingClient).invokeOneway(anyString(), any(RemotingCommand.class), anyLong());
        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
            3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
        assertThat(sendResult).isNull();
    }

    @Test
    public void testSendMessageOneWay_WithException() throws RemotingException, InterruptedException, MQBrokerException {
        doThrow(new RemotingTimeoutException("Remoting Exception in Test")).when(remotingClient).invokeOneway(anyString(), any(RemotingCommand.class), anyLong());
        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
                3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(RemotingException.class);
        } catch (RemotingException e) {
            assertThat(e).hasMessage("Remoting Exception in Test");
        }

        doThrow(new InterruptedException("Interrupted Exception in Test")).when(remotingClient).invokeOneway(anyString(), any(RemotingCommand.class), anyLong());
        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
                3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(InterruptedException.class);
        } catch (InterruptedException e) {
            assertThat(e).hasMessage("Interrupted Exception in Test");
        }
    }

    @Test
    public void testSendMessageSync_Success() throws InterruptedException, RemotingException, MQBrokerException {
        doAnswer(mock -> {
            RemotingCommand request = mock.getArgument(1);
            return createSendMessageSuccessResponse(request);
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();

        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, requestHeader,
            3 * 1000, CommunicationMode.SYNC, new SendMessageContext(), defaultMQProducerImpl);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
        assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
    }

    @Test
    public void testSendMessageSync_WithException() throws InterruptedException, RemotingException {
        doAnswer(mock -> {
            RemotingCommand request = mock.getArgument(1);
            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setOpaque(request.getOpaque());
            response.setRemark("Broker is broken.");
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        SendMessageRequestHeader requestHeader = createSendMessageRequestHeader();

        try {
            mqClientAPI.sendMessage(brokerAddr, brokerName, msg, requestHeader,
                3 * 1000, CommunicationMode.SYNC, new SendMessageContext(), defaultMQProducerImpl);
            failBecauseExceptionWasNotThrown(MQBrokerException.class);
        } catch (MQBrokerException e) {
            assertThat(e).hasMessageContaining("Broker is broken.");
        }
    }

    @Test
    public void testSendMessageAsync_Success() throws RemotingException, InterruptedException, MQBrokerException {
        doNothing().when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
            3 * 1000, CommunicationMode.ASYNC, new SendMessageContext(), defaultMQProducerImpl);
        assertThat(sendResult).isNull();

        doAnswer(mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            responseFuture.setResponseCommand(createSendMessageSuccessResponse(request));
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        SendMessageContext sendMessageContext = new SendMessageContext();
        sendMessageContext.setProducer(new DefaultMQProducerImpl(new DefaultMQProducer()));
        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
            new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                    assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                    assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
                    assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
                }

                @Override
                public void onException(Throwable e) {
                }
            },
            null, null, 0, sendMessageContext, defaultMQProducerImpl);
    }

    @Test
    public void testSendMessageAsync_WithException() throws RemotingException, InterruptedException, MQBrokerException {
        doThrow(new RemotingTimeoutException("Remoting Exception in Test")).when(remotingClient)
            .invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        SendMessageContext sendMessageContext = new SendMessageContext();
        sendMessageContext.setProducer(new DefaultMQProducerImpl(new DefaultMQProducer()));
        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                    }
                    @Override
                    public void onException(Throwable e) {
                        assertThat(e).hasMessage("Remoting Exception in Test");
                    }
                }, null, null, 0, sendMessageContext, defaultMQProducerImpl);

        doThrow(new InterruptedException("Interrupted Exception in Test")).when(remotingClient)
            .invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                    }
                    @Override
                    public void onException(Throwable e) {
                        assertThat(e).hasMessage("Interrupted Exception in Test");
                    }
                }, null, null, 0, sendMessageContext, defaultMQProducerImpl);
    }

    @Test
    public void testResumeCheckHalfMessage_WithException() throws RemotingException, InterruptedException {
        doAnswer(mock -> {
            RemotingCommand request = mock.getArgument(1);
            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setOpaque(request.getOpaque());
            response.setRemark("Put message back to RMQ_SYS_TRANS_HALF_TOPIC failed.");
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        boolean result = mqClientAPI.resumeCheckHalfMessage(brokerAddr, "topic,", "test", 3000);
        assertThat(result).isEqualTo(false);
    }

    @Test
    public void testResumeCheckHalfMessage_Success() throws InterruptedException, RemotingException {
        doAnswer(mock -> {
            RemotingCommand request = mock.getArgument(1);
            return createResumeSuccessResponse(request);
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        boolean result = mqClientAPI.resumeCheckHalfMessage(brokerAddr, "topic", "test", 3000);

        assertThat(result).isEqualTo(true);
    }

    @Test
    public void testSendMessageTypeofReply() throws Exception {
        doAnswer(mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            responseFuture.setResponseCommand(createSendMessageSuccessResponse(request));
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(ArgumentMatchers.anyString(), ArgumentMatchers.any(RemotingCommand.class), ArgumentMatchers.anyLong(), ArgumentMatchers.any(InvokeCallback.class));
        SendMessageContext sendMessageContext = new SendMessageContext();
        sendMessageContext.setProducer(new DefaultMQProducerImpl(new DefaultMQProducer()));
        msg.getProperties().put("MSG_TYPE", "reply");
        mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(), 3 * 1000, CommunicationMode.ASYNC,
            new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                    assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                    assertThat(sendResult.getQueueOffset()).isEqualTo(123L);
                    assertThat(sendResult.getMessageQueue().getQueueId()).isEqualTo(1);
                }

                @Override
                public void onException(Throwable e) {
                }
            }, null, null, 0, sendMessageContext, defaultMQProducerImpl);
    }

    @Test
    public void testQueryAssignment_Success() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            RemotingCommand response = RemotingCommand.createResponseCommand(null);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            QueryAssignmentResponseBody b = new QueryAssignmentResponseBody();
            b.setMessageQueueAssignments(Collections.singleton(new MessageQueueAssignment()));
            response.setBody(b.encode());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());
        Set<MessageQueueAssignment> assignments = mqClientAPI.queryAssignment(brokerAddr, topic, group, clientId, null, MessageModel.CLUSTERING, 10 * 1000);
        assertThat(assignments).size().isEqualTo(1);
    }

    @Test
    public void testPopMessageAsync_Success() throws Exception {
        final long popTime = System.currentTimeMillis();
        final int invisibleTime = 10 * 1000;
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());

            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
            responseHeader.setInvisibleTime(invisibleTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(0);
            responseHeader.setRestNum(1);
            StringBuilder startOffsetInfo = new StringBuilder(64);
            ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, 0, 0L);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            StringBuilder msgOffsetInfo = new StringBuilder(64);
            ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, 0, Collections.singletonList(0L));
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            response.setRemark("FOUND");
            response.makeCustomHeaderToNet();

            MessageExt message = new MessageExt();
            message.setQueueId(0);
            message.setFlag(12);
            message.setQueueOffset(0L);
            message.setCommitLogOffset(100L);
            message.setSysFlag(0);
            message.setBornTimestamp(System.currentTimeMillis());
            message.setBornHost(new InetSocketAddress("127.0.0.1", 10));
            message.setStoreTimestamp(System.currentTimeMillis());
            message.setStoreHost(new InetSocketAddress("127.0.0.1", 11));
            message.setBody("body".getBytes());
            message.setTopic(topic);
            message.putUserProperty("key", "value");
            response.setBody(MessageDecoder.encode(message, false));
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        final CountDownLatch done = new CountDownLatch(1);
        mqClientAPI.popMessageAsync(brokerName, brokerAddr, new PopMessageRequestHeader(), 10 * 1000, new PopCallback() {
            @Override
            public void onSuccess(PopResult popResult) {
                assertThat(popResult.getPopStatus()).isEqualTo(PopStatus.FOUND);
                assertThat(popResult.getRestNum()).isEqualTo(1);
                assertThat(popResult.getInvisibleTime()).isEqualTo(invisibleTime);
                assertThat(popResult.getPopTime()).isEqualTo(popTime);
                assertThat(popResult.getMsgFoundList()).size().isEqualTo(1);
                done.countDown();
            }

            @Override
            public void onException(Throwable e) {
                Assertions.fail("want no exception but got one", e);
                done.countDown();
            }
        });
        done.await();
    }

    @Test
    public void testPopLmqMessage_async() throws Exception {
        final long popTime = System.currentTimeMillis();
        final int invisibleTime = 10 * 1000;
        final String lmqTopic = MixAll.LMQ_PREFIX + "lmq1";
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());

            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
            responseHeader.setInvisibleTime(invisibleTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(0);
            responseHeader.setRestNum(1);
            StringBuilder startOffsetInfo = new StringBuilder(64);
            ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, 0, 0L);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            StringBuilder msgOffsetInfo = new StringBuilder(64);
            ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, 0, Collections.singletonList(0L));
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            response.setRemark("FOUND");
            response.makeCustomHeaderToNet();

            MessageExt message = new MessageExt();
            message.setQueueId(3);
            message.setFlag(0);
            message.setQueueOffset(5L);
            message.setCommitLogOffset(11111L);
            message.setSysFlag(0);
            message.setBornTimestamp(System.currentTimeMillis());
            message.setBornHost(new InetSocketAddress("127.0.0.1", 10));
            message.setStoreTimestamp(System.currentTimeMillis());
            message.setStoreHost(new InetSocketAddress("127.0.0.1", 11));
            message.setBody("body".getBytes());
            message.setTopic(topic);
            message.putUserProperty("key", "value");
            message.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, lmqTopic);
            message.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, String.valueOf(0));
            response.setBody(MessageDecoder.encode(message, false));
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        final CountDownLatch done = new CountDownLatch(1);
        final PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setTopic(lmqTopic);
        mqClientAPI.popMessageAsync(brokerName, brokerAddr, requestHeader, 10 * 1000, new PopCallback() {
            @Override
            public void onSuccess(PopResult popResult) {
                assertThat(popResult.getPopStatus()).isEqualTo(PopStatus.FOUND);
                assertThat(popResult.getRestNum()).isEqualTo(1);
                assertThat(popResult.getInvisibleTime()).isEqualTo(invisibleTime);
                assertThat(popResult.getPopTime()).isEqualTo(popTime);
                assertThat(popResult.getMsgFoundList()).size().isEqualTo(1);
                assertThat(popResult.getMsgFoundList().get(0).getTopic()).isEqualTo(lmqTopic);
                assertThat(popResult.getMsgFoundList().get(0).getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
                    .isEqualTo(lmqTopic);
                done.countDown();
            }

            @Override
            public void onException(Throwable e) {
                Assertions.fail("want no exception but got one", e);
                done.countDown();
            }
        });
        done.await();
    }

    @Test
    public void testPopMultiLmqMessage_async() throws Exception {
        final long popTime = System.currentTimeMillis();
        final int invisibleTime = 10 * 1000;
        final String lmqTopic = MixAll.LMQ_PREFIX + "lmq1";
        final String lmqTopic2 = MixAll.LMQ_PREFIX + "lmq2";
        final String multiDispatch = String.join(MixAll.LMQ_DISPATCH_SEPARATOR, lmqTopic, lmqTopic2);
        final String multiOffset = String.join(MixAll.LMQ_DISPATCH_SEPARATOR, "0", "0");
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());

            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
            responseHeader.setInvisibleTime(invisibleTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(0);
            responseHeader.setRestNum(1);
            StringBuilder startOffsetInfo = new StringBuilder(64);
            ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, topic, 0, 0L);
            responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
            StringBuilder msgOffsetInfo = new StringBuilder(64);
            ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, topic, 0, Collections.singletonList(0L));
            responseHeader.setMsgOffsetInfo(msgOffsetInfo.toString());
            response.setRemark("FOUND");
            response.makeCustomHeaderToNet();

            MessageExt message = new MessageExt();
            message.setQueueId(0);
            message.setFlag(0);
            message.setQueueOffset(10L);
            message.setCommitLogOffset(10000L);
            message.setSysFlag(0);
            message.setBornTimestamp(System.currentTimeMillis());
            message.setBornHost(new InetSocketAddress("127.0.0.1", 10));
            message.setStoreTimestamp(System.currentTimeMillis());
            message.setStoreHost(new InetSocketAddress("127.0.0.1", 11));
            message.setBody("body".getBytes());
            message.setTopic(topic);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_DISPATCH, multiDispatch);
            MessageAccessor.putProperty(message, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, multiOffset);
            response.setBody(MessageDecoder.encode(message, false));
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));
        final CountDownLatch done = new CountDownLatch(1);
        final PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setTopic(lmqTopic);
        mqClientAPI.popMessageAsync(brokerName, brokerAddr, requestHeader, 10 * 1000, new PopCallback() {
            @Override
            public void onSuccess(PopResult popResult) {
                assertThat(popResult.getPopStatus()).isEqualTo(PopStatus.FOUND);
                assertThat(popResult.getRestNum()).isEqualTo(1);
                assertThat(popResult.getInvisibleTime()).isEqualTo(invisibleTime);
                assertThat(popResult.getPopTime()).isEqualTo(popTime);
                assertThat(popResult.getMsgFoundList()).size().isEqualTo(1);
                assertThat(popResult.getMsgFoundList().get(0).getTopic()).isEqualTo(lmqTopic);
                assertThat(popResult.getMsgFoundList().get(0).getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
                    .isEqualTo(multiDispatch);
                assertThat(popResult.getMsgFoundList().get(0).getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET))
                    .isEqualTo(multiOffset);
                done.countDown();
            }

            @Override
            public void onException(Throwable e) {
                Assertions.fail("want no exception but got one", e);
                done.countDown();
            }
        });
        done.await();
    }

    @Test
    public void testAckMessageAsync_Success() throws Exception {
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            response.setOpaque(request.getOpaque());
            response.setCode(ResponseCode.SUCCESS);
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        final CountDownLatch done = new CountDownLatch(1);
        mqClientAPI.ackMessageAsync(brokerAddr, 10 * 1000, new AckCallback() {
            @Override
            public void onSuccess(AckResult ackResult) {
                assertThat(ackResult.getStatus()).isEqualTo(AckStatus.OK);
                done.countDown();
            }

            @Override
            public void onException(Throwable e) {
                Assertions.fail("want no exception but got one", e);
                done.countDown();
            }
        }, new AckMessageRequestHeader());
        done.await();
    }

    @Test
    public void testChangeInvisibleTimeAsync_Success() throws Exception {
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
            response.setOpaque(request.getOpaque());
            response.setCode(ResponseCode.SUCCESS);
            ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) response.readCustomHeader();
            responseHeader.setPopTime(System.currentTimeMillis());
            responseHeader.setInvisibleTime(10 * 1000L);
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        final CountDownLatch done = new CountDownLatch(1);
        ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(0);
        requestHeader.setOffset(0L);
        requestHeader.setInvisibleTime(10 * 1000L);
        mqClientAPI.changeInvisibleTimeAsync(brokerName, brokerAddr, requestHeader, 10 * 1000, new AckCallback() {
            @Override
            public void onSuccess(AckResult ackResult) {
                assertThat(ackResult.getStatus()).isEqualTo(AckStatus.OK);
                done.countDown();
            }

            @Override
            public void onException(Throwable e) {
                Assertions.fail("want no exception but got one", e);
                done.countDown();
            }
        });
        done.await();
    }

    @Test
    public void testSetMessageRequestMode_Success() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            RemotingCommand response = RemotingCommand.createResponseCommand(null);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.setMessageRequestMode(brokerAddr, topic, group, MessageRequestMode.POP, 8, 10 * 1000L);
    }

    @Test
    public void testCreateSubscriptionGroup_Success() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            RemotingCommand response = RemotingCommand.createResponseCommand(null);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.createSubscriptionGroup(brokerAddr, new SubscriptionGroupConfig(), 10000);
    }

    @Test
    public void testCreateTopic_Success() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            RemotingCommand response = RemotingCommand.createResponseCommand(null);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.createTopic(brokerAddr, topic, new TopicConfig(), 10000);
    }

    @Test
    public void testViewMessage() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            RemotingCommand response = RemotingCommand.createResponseCommand(null);
            MessageExt message = new MessageExt();
            message.setQueueId(0);
            message.setFlag(12);
            message.setQueueOffset(0L);
            message.setCommitLogOffset(100L);
            message.setSysFlag(0);
            message.setBornTimestamp(System.currentTimeMillis());
            message.setBornHost(new InetSocketAddress("127.0.0.1", 10));
            message.setStoreTimestamp(System.currentTimeMillis());
            message.setStoreHost(new InetSocketAddress("127.0.0.1", 11));
            message.setBody("body".getBytes());
            message.setTopic(topic);
            message.putUserProperty("key", "value");
            response.setBody(MessageDecoder.encode(message, false));
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        MessageExt messageExt = mqClientAPI.viewMessage(brokerAddr, "topic", 100L, 10000);
        assertThat(messageExt.getTopic()).isEqualTo(topic);
    }

    @Test
    public void testSearchOffset() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
            final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(100L);
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long offset = mqClientAPI.searchOffset(brokerAddr, topic, 0, System.currentTimeMillis() - 1000, 10000);
        assertThat(offset).isEqualTo(100L);
    }

    @Test
    public void testGetMaxOffset() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
            final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(100L);
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long offset = mqClientAPI.getMaxOffset(brokerAddr, new MessageQueue(topic, brokerName, 0), 10000);
        assertThat(offset).isEqualTo(100L);
    }

    @Test
    public void testGetMinOffset() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response = RemotingCommand.createResponseCommand(GetMinOffsetResponseHeader.class);
            final GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(100L);
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long offset = mqClientAPI.getMinOffset(brokerAddr, new MessageQueue(topic, brokerName, 0), 10000);
        assertThat(offset).isEqualTo(100L);
    }

    @Test
    public void testGetEarliestMsgStoretime() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response = RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
            final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
            responseHeader.setTimestamp(100L);
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long t = mqClientAPI.getEarliestMsgStoretime(brokerAddr, new MessageQueue(topic, brokerName, 0), 10000);
        assertThat(t).isEqualTo(100L);
    }

    @Test
    public void testQueryConsumerOffset() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
            final QueryConsumerOffsetResponseHeader responseHeader =
                    (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(100L);
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long t = mqClientAPI.queryConsumerOffset(brokerAddr, new QueryConsumerOffsetRequestHeader(), 1000);
        assertThat(t).isEqualTo(100L);
    }

    @Test
    public void testUpdateConsumerOffset() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.updateConsumerOffset(brokerAddr, new UpdateConsumerOffsetRequestHeader(), 1000);
    }

    @Test
    public void testGetConsumerIdListByGroup() throws Exception {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
            GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
            body.setConsumerIdList(Collections.singletonList("consumer1"));
            response.setBody(body.encode());
            response.makeCustomHeaderToNet();
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());
        List<String> consumerIdList = mqClientAPI.getConsumerIdListByGroup(brokerAddr, group, 10000);
        assertThat(consumerIdList).size().isGreaterThan(0);
    }

    private RemotingCommand createResumeSuccessResponse(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        return response;
    }

    private RemotingCommand createSendMessageSuccessResponse(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        responseHeader.setMsgId("123");
        responseHeader.setQueueId(1);
        responseHeader.setQueueOffset(123L);

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, "RegionHZ");
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, "true");
        response.addExtField("queueId", String.valueOf(responseHeader.getQueueId()));
        response.addExtField("msgId", responseHeader.getMsgId());
        response.addExtField("queueOffset", String.valueOf(responseHeader.getQueueOffset()));
        return response;
    }

    private RemotingCommand createSuccessResponse4UpdateAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark(null);
        return response;
    }

    private RemotingCommand createSuccessResponse4DeleteAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark(null);
        return response;
    }

    private SendMessageRequestHeader createSendMessageRequestHeader() {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setTopic(topic);
        requestHeader.setProducerGroup(group);
        requestHeader.setQueueId(1);
        requestHeader.setMaxReconsumeTimes(10);
        return requestHeader;
    }

    @Test
    public void testAddWritePermOfBroker() throws Exception {
        doAnswer(invocationOnMock -> {
            RemotingCommand request = invocationOnMock.getArgument(1);
            if (request.getCode() != RequestCode.ADD_WRITE_PERM_OF_BROKER) {
                return null;
            }

            RemotingCommand response = RemotingCommand.createResponseCommand(AddWritePermOfBrokerResponseHeader.class);
            AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.readCustomHeader();
            response.setCode(ResponseCode.SUCCESS);
            responseHeader.setAddTopicCount(7);
            response.addExtField("addTopicCount", String.valueOf(responseHeader.getAddTopicCount()));
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        int topicCnt = mqClientAPI.addWritePermOfBroker("127.0.0.1", "default-broker", 1000);
        assertThat(topicCnt).isEqualTo(7);
    }

    @Test
    public void testCreateTopicList_Success() throws RemotingException, InterruptedException, MQClientException {
        doAnswer((Answer<RemotingCommand>) mock -> {
            RemotingCommand request = mock.getArgument(1);

            RemotingCommand response = RemotingCommand.createResponseCommand(null);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            return response;
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        final List<TopicConfig> topicConfigList = new LinkedList<>();
        for (int i = 0; i < 16; i++) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setTopicName("Topic" + i);
            topicConfigList.add(topicConfig);
        }

        mqClientAPI.createTopicList(brokerAddr, topicConfigList, 10000);
    }

    @Test
    public void assertFetchNameServerAddr() throws NoSuchFieldException, IllegalAccessException {
        setTopAddressing();
        assertEquals(defaultNsAddr, mqClientAPI.fetchNameServerAddr());
    }

    @Test
    public void assertOnNameServerAddressChange() {
        assertEquals(defaultNsAddr, mqClientAPI.onNameServerAddressChange(defaultNsAddr));
    }

    @Test
    public void assertPullMessage() throws MQBrokerException, RemotingException, InterruptedException {
        PullMessageRequestHeader requestHeader = mock(PullMessageRequestHeader.class);
        mockInvokeSync();
        PullCallback callback = mock(PullCallback.class);
        PullMessageResponseHeader responseHeader = mock(PullMessageResponseHeader.class);
        setResponseHeader(responseHeader);
        when(responseHeader.getNextBeginOffset()).thenReturn(1L);
        when(responseHeader.getMinOffset()).thenReturn(1L);
        when(responseHeader.getMaxOffset()).thenReturn(10L);
        when(responseHeader.getSuggestWhichBrokerId()).thenReturn(MixAll.MASTER_ID);
        PullResult actual = mqClientAPI.pullMessage(defaultBrokerAddr, requestHeader, defaultTimeout, CommunicationMode.SYNC, callback);
        assertNotNull(actual);
        assertEquals(1L, actual.getNextBeginOffset());
        assertEquals(1L, actual.getMinOffset());
        assertEquals(10L, actual.getMaxOffset());
        assertEquals(PullStatus.FOUND, actual.getPullStatus());
        assertNull(actual.getMsgFoundList());
    }

    @Test
    public void testBatchAckMessageAsync() throws MQBrokerException, RemotingException, InterruptedException {
        AckCallback callback = mock(AckCallback.class);
        List<String> extraInfoList = new ArrayList<>();
        extraInfoList.add(String.format("%s %s %s %s %s %s %d %d", "1", "2", "3", "4", "5", brokerName, 7, 8));
        mqClientAPI.batchAckMessageAsync(defaultBrokerAddr, defaultTimeout, callback, defaultTopic, "", extraInfoList);
    }

    @Test
    public void assertSearchOffset() throws MQBrokerException, RemotingException, InterruptedException {
        mockInvokeSync();
        SearchOffsetResponseHeader responseHeader = mock(SearchOffsetResponseHeader.class);
        when(responseHeader.getOffset()).thenReturn(1L);
        setResponseHeader(responseHeader);
        assertEquals(1L, mqClientAPI.searchOffset(defaultBrokerAddr, new MessageQueue(), System.currentTimeMillis(), defaultTimeout));
    }

    @Test
    public void testUpdateConsumerOffsetOneway() throws RemotingException, InterruptedException {
        UpdateConsumerOffsetRequestHeader requestHeader = mock(UpdateConsumerOffsetRequestHeader.class);
        mqClientAPI.updateConsumerOffsetOneway(defaultBrokerAddr, requestHeader, defaultTimeout);
    }

    @Test
    public void assertSendHeartbeat() throws MQBrokerException, RemotingException, InterruptedException {
        mockInvokeSync();
        HeartbeatData heartbeatData = new HeartbeatData();
        assertEquals(1, mqClientAPI.sendHeartbeat(defaultBrokerAddr, heartbeatData, defaultTimeout));
    }

    @Test
    public void assertSendHeartbeatV2() throws MQBrokerException, RemotingException, InterruptedException {
        mockInvokeSync();
        HeartbeatData heartbeatData = new HeartbeatData();
        HeartbeatV2Result actual = mqClientAPI.sendHeartbeatV2(defaultBrokerAddr, heartbeatData, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getVersion());
        assertFalse(actual.isSubChange());
        assertFalse(actual.isSupportV2());
    }

    @Test
    public void testUnregisterClient() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.unregisterClient(defaultBrokerAddr, "", "", "", defaultTimeout);
    }

    @Test
    public void testEndTransactionOneway() throws RemotingException, InterruptedException {
        mockInvokeSync();
        EndTransactionRequestHeader requestHeader = mock(EndTransactionRequestHeader.class);
        mqClientAPI.endTransactionOneway(defaultBrokerAddr, requestHeader, "", defaultTimeout);
    }

    @Test
    public void testQueryMessage() throws MQBrokerException, RemotingException, InterruptedException {
        QueryMessageRequestHeader requestHeader = mock(QueryMessageRequestHeader.class);
        InvokeCallback callback = mock(InvokeCallback.class);
        mqClientAPI.queryMessage(defaultBrokerAddr, requestHeader, defaultTimeout, callback, false);
    }

    @Test
    public void testRegisterClient() throws RemotingException, InterruptedException {
        mockInvokeSync();
        HeartbeatData heartbeatData = new HeartbeatData();
        assertTrue(mqClientAPI.registerClient(defaultBrokerAddr, heartbeatData, defaultTimeout));
    }

    @Test
    public void testConsumerSendMessageBack() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        MessageExt messageExt = mock(MessageExt.class);
        mqClientAPI.consumerSendMessageBack(defaultBrokerAddr, brokerName, messageExt, "", 1, defaultTimeout, 1000);
    }

    @Test
    public void assertLockBatchMQ() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        LockBatchRequestBody responseBody = new LockBatchRequestBody();
        setResponseBody(responseBody);
        Set<MessageQueue> actual = mqClientAPI.lockBatchMQ(defaultBrokerAddr, responseBody, defaultTimeout);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void testUnlockBatchMQ() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        UnlockBatchRequestBody unlockBatchRequestBody = new UnlockBatchRequestBody();
        mqClientAPI.unlockBatchMQ(defaultBrokerAddr, unlockBatchRequestBody, defaultTimeout, false);
    }

    @Test
    public void assertGetTopicStatsInfo() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        TopicStatsTable responseBody = new TopicStatsTable();
        MessageQueue messageQueue = new MessageQueue();
        TopicOffset topicOffset = new TopicOffset();
        responseBody.getOffsetTable().put(messageQueue, topicOffset);
        setResponseBody(responseBody);
        TopicStatsTable actual = mqClientAPI.getTopicStatsInfo(defaultBrokerAddr, defaultTopic, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getOffsetTable().size());
    }

    @Test
    public void assertGetConsumeStats() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        ConsumeStats responseBody = new ConsumeStats();
        responseBody.setConsumeTps(1000);
        setResponseBody(responseBody);
        ConsumeStats actual = mqClientAPI.getConsumeStats(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals(1000, actual.getConsumeTps(), 0.0);
    }

    @Test
    public void assertGetProducerConnectionList() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        ProducerConnection responseBody = new ProducerConnection();
        responseBody.getConnectionSet().add(new Connection());
        setResponseBody(responseBody);
        ProducerConnection actual = mqClientAPI.getProducerConnectionList(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getConnectionSet().size());
    }

    @Test
    public void assertGetAllProducerInfo() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        Map<String, List<ProducerInfo>> data = new HashMap<>();
        data.put("key", Collections.emptyList());
        ProducerTableInfo responseBody = new ProducerTableInfo(data);
        setResponseBody(responseBody);
        ProducerTableInfo actual = mqClientAPI.getAllProducerInfo(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getData().size());
    }

    @Test
    public void assertGetConsumerConnectionList() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        ConsumerConnection responseBody = new ConsumerConnection();
        responseBody.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        responseBody.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        responseBody.setMessageModel(MessageModel.CLUSTERING);
        setResponseBody(responseBody);
        ConsumerConnection actual = mqClientAPI.getConsumerConnectionList(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals(ConsumeType.CONSUME_ACTIVELY, actual.getConsumeType());
        assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, actual.getConsumeFromWhere());
        assertEquals(MessageModel.CLUSTERING, actual.getMessageModel());
    }

    @Test
    public void assertGetBrokerRuntimeInfo() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        KVTable responseBody = new KVTable();
        responseBody.getTable().put("key", "value");
        setResponseBody(responseBody);
        KVTable actual = mqClientAPI.getBrokerRuntimeInfo(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTable().size());
    }

    @Test
    public void testAddBroker() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.addBroker(defaultBrokerAddr, "", defaultTimeout);
    }

    @Test
    public void testRemoveBroker() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.removeBroker(defaultBrokerAddr, clusterName, brokerName, MixAll.MASTER_ID, defaultTimeout);
    }

    @Test
    public void testUpdateBrokerConfig() throws RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException, MQClientException {
        mockInvokeSync();
        mqClientAPI.updateBrokerConfig(defaultBrokerAddr, createProperties(), defaultTimeout);
    }

    @Test
    public void assertGetBrokerConfig() throws RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        mockInvokeSync();
        setResponseBody("{\"key\":\"value\"}");
        Properties actual = mqClientAPI.getBrokerConfig(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
    }

    @Test
    public void testUpdateColdDataFlowCtrGroupConfig() throws RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        mockInvokeSync();
        Properties props = new Properties();
        mqClientAPI.updateColdDataFlowCtrGroupConfig(defaultBrokerAddr, props, defaultTimeout);
    }

    @Test
    public void testRemoveColdDataFlowCtrGroupConfig() throws RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        mockInvokeSync();
        mqClientAPI.removeColdDataFlowCtrGroupConfig(defaultBrokerAddr, "", defaultTimeout);
    }

    @Test
    public void assertGetColdDataFlowCtrInfo() throws RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        mockInvokeSync();
        setResponseBody("{\"key\":\"value\"}");
        String actual = mqClientAPI.getColdDataFlowCtrInfo(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals("\"{\\\"key\\\":\\\"value\\\"}\"", actual);
    }

    @Test
    public void assertSetCommitLogReadAheadMode() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        when(response.getRemark()).thenReturn("remark");
        String actual = mqClientAPI.setCommitLogReadAheadMode(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals("remark", actual);
    }

    @Test
    public void assertGetBrokerClusterInfo() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        ClusterInfo responseBody = new ClusterInfo();
        Map<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put(clusterName, new HashSet<>());
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put(brokerName, new BrokerData());
        responseBody.setClusterAddrTable(clusterAddrTable);
        responseBody.setBrokerAddrTable(brokerAddrTable);
        setResponseBody(responseBody);
        ClusterInfo actual = mqClientAPI.getBrokerClusterInfo(defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getClusterAddrTable().size());
        assertEquals(1, actual.getBrokerAddrTable().size());
    }

    @Test
    public void assertGetDefaultTopicRouteInfoFromNameServer() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicRouteData responseBody = new TopicRouteData();
        responseBody.getQueueDatas().add(new QueueData());
        responseBody.getBrokerDatas().add(new BrokerData());
        responseBody.getFilterServerTable().put("key", Collections.emptyList());
        Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker = new HashMap<>();
        topicQueueMappingByBroker.put("key", new TopicQueueMappingInfo());
        responseBody.setTopicQueueMappingByBroker(topicQueueMappingByBroker);
        setResponseBody(responseBody);
        TopicRouteData actual = mqClientAPI.getDefaultTopicRouteInfoFromNameServer(defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getQueueDatas().size());
        assertEquals(1, actual.getBrokerDatas().size());
        assertEquals(1, actual.getFilterServerTable().size());
        assertEquals(1, actual.getTopicQueueMappingByBroker().size());
    }

    @Test
    public void assertGetTopicRouteInfoFromNameServer() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicRouteData responseBody = new TopicRouteData();
        responseBody.getQueueDatas().add(new QueueData());
        responseBody.getBrokerDatas().add(new BrokerData());
        responseBody.getFilterServerTable().put("key", Collections.emptyList());
        Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker = new HashMap<>();
        topicQueueMappingByBroker.put("key", new TopicQueueMappingInfo());
        responseBody.setTopicQueueMappingByBroker(topicQueueMappingByBroker);
        setResponseBody(responseBody);
        TopicRouteData actual = mqClientAPI.getTopicRouteInfoFromNameServer(defaultTopic, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getQueueDatas().size());
        assertEquals(1, actual.getBrokerDatas().size());
        assertEquals(1, actual.getFilterServerTable().size());
        assertEquals(1, actual.getTopicQueueMappingByBroker().size());
    }

    @Test
    public void assertGetTopicListFromNameServer() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.setBrokerAddr(defaultBrokerAddr);
        responseBody.getTopicList().add(defaultTopic);
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getTopicListFromNameServer(defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicList().size());
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
    }
    
    @Test
    public void assertGetRetryTopicListFromNameServer() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.setBrokerAddr(defaultBrokerAddr);
        responseBody.getTopicList().add("%RETRY%group");
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getRetryTopicListFromNameServer(defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicList().size());
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
    }

    @Test
    public void assertWipeWritePermOfBroker() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        WipeWritePermOfBrokerResponseHeader responseHeader = mock(WipeWritePermOfBrokerResponseHeader.class);
        when(responseHeader.getWipeTopicCount()).thenReturn(1);
        setResponseHeader(responseHeader);
        assertEquals(1, mqClientAPI.wipeWritePermOfBroker(defaultNsAddr, brokerName, defaultTimeout));
    }

    @Test
    public void testDeleteTopicInBroker() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        mqClientAPI.deleteTopicInBroker(defaultBrokerAddr, defaultTopic, defaultTimeout);
    }

    @Test
    public void testDeleteTopicInNameServer() throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.deleteTopicInNameServer(defaultNsAddr, defaultTopic, defaultTimeout);
        mqClientAPI.deleteTopicInNameServer(defaultNsAddr, clusterName, defaultTopic, defaultTimeout);
    }

    @Test
    public void testDeleteSubscriptionGroup() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        mqClientAPI.deleteSubscriptionGroup(defaultBrokerAddr, "", true, defaultTimeout);
    }

    @Test
    public void assertGetKVConfigValue() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        GetKVConfigResponseHeader responseHeader = mock(GetKVConfigResponseHeader.class);
        when(responseHeader.getValue()).thenReturn("value");
        setResponseHeader(responseHeader);
        assertEquals("value", mqClientAPI.getKVConfigValue("", "", defaultTimeout));
    }

    @Test
    public void testPutKVConfigValue() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        mqClientAPI.putKVConfigValue("", "", "", defaultTimeout);
    }

    @Test
    public void testDeleteKVConfigValue() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        mqClientAPI.deleteKVConfigValue("", "", defaultTimeout);
    }

    @Test
    public void assertGetKVListByNamespace() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        KVTable responseBody = new KVTable();
        responseBody.getTable().put("key", "value");
        setResponseBody(responseBody);
        KVTable actual = mqClientAPI.getKVListByNamespace("", defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTable().size());
    }

    @Test
    public void assertInvokeBrokerToResetOffset() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        ResetOffsetBody responseBody = new ResetOffsetBody();
        responseBody.getOffsetTable().put(new MessageQueue(), 1L);
        setResponseBody(responseBody);
        Map<MessageQueue, Long> actual = mqClientAPI.invokeBrokerToResetOffset(defaultBrokerAddr, defaultTopic, "", System.currentTimeMillis(), false, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
        actual = mqClientAPI.invokeBrokerToResetOffset(defaultBrokerAddr, defaultTopic, "", System.currentTimeMillis(), 1, 1L, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
    }

    @Test
    public void assertInvokeBrokerToGetConsumerStatus() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        GetConsumerStatusBody responseBody = new GetConsumerStatusBody();
        responseBody.getConsumerTable().put("key", new HashMap<>());
        responseBody.getMessageQueueTable().put(new MessageQueue(), 1L);
        setResponseBody(responseBody);
        Map<String, Map<MessageQueue, Long>> actual = mqClientAPI.invokeBrokerToGetConsumerStatus(defaultBrokerAddr, defaultTopic, "", "", defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
    }

    @Test
    public void assertQueryTopicConsumeByWho() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        GroupList responseBody = new GroupList();
        responseBody.getGroupList().add("");
        setResponseBody(responseBody);
        GroupList actual = mqClientAPI.queryTopicConsumeByWho(defaultBrokerAddr, defaultTopic, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getGroupList().size());
    }

    @Test
    public void assertQueryTopicsByConsumer() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.getTopicList().add(defaultTopic);
        responseBody.setBrokerAddr(defaultBrokerAddr);
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.queryTopicsByConsumer(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicList().size());
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
    }

    @Test
    public void assertQuerySubscriptionByConsumer() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        QuerySubscriptionResponseBody responseBody = new QuerySubscriptionResponseBody();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(defaultTopic);
        responseBody.setSubscriptionData(subscriptionData);
        setResponseBody(responseBody);
        SubscriptionData actual = mqClientAPI.querySubscriptionByConsumer(defaultBrokerAddr, group, defaultTopic, defaultTimeout);
        assertNotNull(actual);
        assertEquals(defaultTopic, actual.getTopic());
    }

    @Test
    public void assertQueryConsumeTimeSpan() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        QueryConsumeTimeSpanBody responseBody = new QueryConsumeTimeSpanBody();
        responseBody.getConsumeTimeSpanSet().add(new QueueTimeSpan());
        setResponseBody(responseBody);
        List<QueueTimeSpan> actual = mqClientAPI.queryConsumeTimeSpan(defaultBrokerAddr, defaultTopic, group, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
    }

    @Test
    public void assertGetTopicsByCluster() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.setBrokerAddr(defaultBrokerAddr);
        responseBody.setTopicList(Collections.singleton(defaultTopic));
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getTopicsByCluster(clusterName, defaultTimeout);
        assertNotNull(actual);
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
        assertEquals(1, actual.getTopicList().size());
        assertTrue(actual.getTopicList().contains(defaultTopic));
    }

    @Test
    public void assertGetSystemTopicList() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.setBrokerAddr(defaultBrokerAddr);
        responseBody.setTopicList(Collections.singleton(defaultTopic));
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getSystemTopicList(defaultTimeout);
        assertNotNull(actual);
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
        assertEquals(1, actual.getTopicList().size());
        assertTrue(actual.getTopicList().contains(defaultTopic));
    }

    @Test
    public void assertGetSystemTopicListFromBroker() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.setBrokerAddr(defaultBrokerAddr);
        responseBody.setTopicList(Collections.singleton(defaultTopic));
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getSystemTopicListFromBroker(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
        assertEquals(1, actual.getTopicList().size());
        assertTrue(actual.getTopicList().contains(defaultTopic));
    }

    @Test
    public void assertCleanExpiredConsumeQueue() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        assertTrue(mqClientAPI.cleanExpiredConsumeQueue(defaultBrokerAddr, defaultTimeout));
    }

    @Test
    public void assertDeleteExpiredCommitLog() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        assertTrue(mqClientAPI.deleteExpiredCommitLog(defaultBrokerAddr, defaultTimeout));
    }

    @Test
    public void assertCleanUnusedTopicByAddr() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        assertTrue(mqClientAPI.cleanUnusedTopicByAddr(defaultBrokerAddr, defaultTimeout));
    }

    @Test
    public void assertGetConsumerRunningInfo() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        ConsumerRunningInfo responseBody = new ConsumerRunningInfo();
        responseBody.setJstack("jstack");
        responseBody.getUserConsumerInfo().put("key", "value");
        setResponseBody(responseBody);
        ConsumerRunningInfo actual = mqClientAPI.getConsumerRunningInfo(defaultBrokerAddr, group, clientId, false, defaultTimeout);
        assertNotNull(actual);
        assertEquals("jstack", actual.getJstack());
        assertEquals(1, actual.getUserConsumerInfo().size());
    }

    @Test
    public void assertConsumeMessageDirectly() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        ConsumeMessageDirectlyResult responseBody = new ConsumeMessageDirectlyResult();
        responseBody.setAutoCommit(true);
        responseBody.setRemark("remark");
        setResponseBody(responseBody);
        ConsumeMessageDirectlyResult actual = mqClientAPI.consumeMessageDirectly(defaultBrokerAddr, group, clientId, topic, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals("remark", actual.getRemark());
        assertTrue(actual.isAutoCommit());
    }

    @Test
    public void assertQueryCorrectionOffset() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        QueryCorrectionOffsetBody responseBody = new QueryCorrectionOffsetBody();
        responseBody.getCorrectionOffsets().put(1, 1L);
        setResponseBody(responseBody);
        Map<Integer, Long> actual = mqClientAPI.queryCorrectionOffset(defaultBrokerAddr, topic, group, new HashSet<>(), defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
        assertTrue(actual.containsKey(1));
        assertTrue(actual.containsValue(1L));
    }

    @Test
    public void assertGetUnitTopicList() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.getTopicList().add(defaultTopic);
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getUnitTopicList(false, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicList().size());
    }

    @Test
    public void assertGetHasUnitSubTopicList() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.getTopicList().add(defaultTopic);
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getHasUnitSubTopicList(false, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicList().size());
    }

    @Test
    public void assertGetHasUnitSubUnUnitTopicList() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        TopicList responseBody = new TopicList();
        responseBody.getTopicList().add(defaultTopic);
        setResponseBody(responseBody);
        TopicList actual = mqClientAPI.getHasUnitSubUnUnitTopicList(false, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicList().size());
    }

    @Test
    public void testCloneGroupOffset() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        mqClientAPI.cloneGroupOffset(defaultBrokerAddr, "", "", defaultTopic, false, defaultTimeout);
    }

    @Test
    public void assertViewBrokerStatsData() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        BrokerStatsData responseBody = new BrokerStatsData();
        responseBody.setStatsDay(new BrokerStatsItem());
        setResponseBody(responseBody);
        BrokerStatsData actual = mqClientAPI.viewBrokerStatsData(defaultBrokerAddr, "", "", defaultTimeout);
        assertNotNull(actual);
        assertNotNull(actual.getStatsDay());
    }

    @Test
    public void assertGetClusterList() {
        Set<String> actual = mqClientAPI.getClusterList(topic, defaultTimeout);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void assertFetchConsumeStatsInBroker() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        ConsumeStatsList responseBody = new ConsumeStatsList();
        responseBody.setBrokerAddr(defaultBrokerAddr);
        responseBody.getConsumeStatsList().add(new HashMap<>());
        setResponseBody(responseBody);
        ConsumeStatsList actual = mqClientAPI.fetchConsumeStatsInBroker(defaultBrokerAddr, false, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getConsumeStatsList().size());
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
    }

    @Test
    public void assertGetAllSubscriptionGroupForSubscriptionGroupWrapper() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        SubscriptionGroupWrapper responseBody = new SubscriptionGroupWrapper();
        responseBody.getSubscriptionGroupTable().put("key", new SubscriptionGroupConfig());
        setResponseBody(responseBody);
        SubscriptionGroupWrapper actual = mqClientAPI.getAllSubscriptionGroup(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getSubscriptionGroupTable().size());
        assertNotNull(actual.getDataVersion());
        assertEquals(0, actual.getDataVersion().getStateVersion());
    }

    @Test
    public void assertGetAllSubscriptionGroupForSubscriptionGroupConfig() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        SubscriptionGroupConfig responseBody = new SubscriptionGroupConfig();
        responseBody.setGroupName(group);
        responseBody.setBrokerId(MixAll.MASTER_ID);
        setResponseBody(responseBody);
        SubscriptionGroupConfig actual = mqClientAPI.getSubscriptionGroupConfig(defaultBrokerAddr, group, defaultTimeout);
        assertNotNull(actual);
        assertEquals(group, actual.getGroupName());
        assertEquals(MixAll.MASTER_ID, actual.getBrokerId());
    }

    @Test
    public void assertGetAllTopicConfig() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        TopicConfigSerializeWrapper responseBody = new TopicConfigSerializeWrapper();
        responseBody.getTopicConfigTable().put("key", new TopicConfig());
        setResponseBody(responseBody);
        TopicConfigSerializeWrapper actual = mqClientAPI.getAllTopicConfig(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getTopicConfigTable().size());
        assertNotNull(actual.getDataVersion());
        assertEquals(0, actual.getDataVersion().getStateVersion());
    }

    @Test
    public void testUpdateNameServerConfig() throws RemotingException, InterruptedException, MQClientException, UnsupportedEncodingException {
        mockInvokeSync();
        mqClientAPI.updateNameServerConfig(createProperties(), Collections.singletonList(defaultNsAddr), defaultTimeout);
    }

    @Test
    public void assertGetNameServerConfig() throws RemotingException, InterruptedException, UnsupportedEncodingException, MQClientException {
        mockInvokeSync();
        setResponseBody("{\"key\":\"value\"}");
        Map<String, Properties> actual = mqClientAPI.getNameServerConfig(Collections.singletonList(defaultNsAddr), defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.size());
        assertTrue(actual.containsKey(defaultNsAddr));
    }

    @Test
    public void assertQueryConsumeQueue() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        QueryConsumeQueueResponseBody responseBody = new QueryConsumeQueueResponseBody();
        responseBody.setQueueData(Collections.singletonList(new ConsumeQueueData()));
        setResponseBody(responseBody);
        QueryConsumeQueueResponseBody actual = mqClientAPI.queryConsumeQueue(defaultBrokerAddr, defaultTopic, 1, 1, 1, group, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1, actual.getQueueData().size());
    }

    @Test
    public void testCheckClientInBroker() throws RemotingException, InterruptedException, MQClientException {
        mockInvokeSync();
        mqClientAPI.checkClientInBroker(defaultBrokerAddr, group, clientId, new SubscriptionData(), defaultTimeout);
    }

    @Test
    public void assertGetTopicConfig() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        TopicConfigAndQueueMapping responseBody = new TopicConfigAndQueueMapping(new TopicConfig(), new TopicQueueMappingDetail());
        setResponseBody(responseBody);
        TopicConfigAndQueueMapping actual = mqClientAPI.getTopicConfig(defaultBrokerAddr, defaultTopic, defaultTimeout);
        assertNotNull(actual);
        assertNotNull(actual.getMappingDetail());
    }

    @Test
    public void testCreateStaticTopic() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.createStaticTopic(defaultBrokerAddr, defaultTopic, new TopicConfig(), new TopicQueueMappingDetail(), false, defaultTimeout);
    }

    @Test
    public void assertUpdateAndGetGroupForbidden() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        GroupForbidden responseBody = new GroupForbidden();
        responseBody.setGroup(group);
        responseBody.setTopic(defaultTopic);
        setResponseBody(responseBody);
        GroupForbidden actual = mqClientAPI.updateAndGetGroupForbidden(defaultBrokerAddr, new UpdateGroupForbiddenRequestHeader(), defaultTimeout);
        assertNotNull(actual);
        assertEquals(group, actual.getGroup());
        assertEquals(defaultTopic, actual.getTopic());
    }

    @Test
    public void testResetMasterFlushOffset() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.resetMasterFlushOffset(defaultBrokerAddr, 1L);
    }

    @Test
    public void assertGetBrokerHAStatus() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        HARuntimeInfo responseBody = new HARuntimeInfo();
        responseBody.setMaster(true);
        responseBody.setMasterCommitLogMaxOffset(1L);
        setResponseBody(responseBody);
        HARuntimeInfo actual = mqClientAPI.getBrokerHAStatus(defaultBrokerAddr, defaultTimeout);
        assertNotNull(actual);
        assertEquals(1L, actual.getMasterCommitLogMaxOffset());
        assertTrue(actual.isMaster());
    }

    @Test
    public void assertGetControllerMetaData() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        GetMetaDataResponseHeader responseHeader = new GetMetaDataResponseHeader();
        responseHeader.setGroup(group);
        responseHeader.setIsLeader(true);
        setResponseHeader(responseHeader);
        GetMetaDataResponseHeader actual = mqClientAPI.getControllerMetaData(defaultBrokerAddr);
        assertNotNull(actual);
        assertEquals(group, actual.getGroup());
        assertTrue(actual.isLeader());
    }

    @Test
    public void assertGetInSyncStateData() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        BrokerReplicasInfo responseBody = new BrokerReplicasInfo();
        BrokerReplicasInfo.ReplicasInfo replicasInfo = new BrokerReplicasInfo.ReplicasInfo(MixAll.MASTER_ID, defaultBrokerAddr, 1, 1, Collections.emptyList(), Collections.emptyList());
        responseBody.getReplicasInfoTable().put("key", replicasInfo);
        GetMetaDataResponseHeader responseHeader = new GetMetaDataResponseHeader();
        responseHeader.setControllerLeaderAddress(defaultBrokerAddr);
        setResponseHeader(responseHeader);
        setResponseBody(responseBody);
        BrokerReplicasInfo actual = mqClientAPI.getInSyncStateData(defaultBrokerAddr, Collections.singletonList(defaultBrokerAddr));
        assertNotNull(actual);
        assertEquals(1L, actual.getReplicasInfoTable().size());
    }

    @Test
    public void assertGetBrokerEpochCache() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        EpochEntryCache responseBody = new EpochEntryCache(clusterName, brokerName, MixAll.MASTER_ID, Collections.emptyList(), 1);
        setResponseBody(responseBody);
        EpochEntryCache actual = mqClientAPI.getBrokerEpochCache(defaultBrokerAddr);
        assertNotNull(actual);
        assertEquals(1L, actual.getMaxOffset());
        assertEquals(MixAll.MASTER_ID, actual.getBrokerId());
        assertEquals(brokerName, actual.getBrokerName());
        assertEquals(clusterName, actual.getClusterName());
    }

    @Test
    public void assertGetControllerConfig() throws RemotingException, InterruptedException, UnsupportedEncodingException, MQClientException {
        mockInvokeSync();
        setResponseBody("{\"key\":\"value\"}");
        Map<String, Properties> actual = mqClientAPI.getControllerConfig(Collections.singletonList(defaultBrokerAddr), defaultTimeout);
        assertNotNull(actual);
        assertEquals(1L, actual.size());
    }

    @Test
    public void testUpdateControllerConfig() throws RemotingException, InterruptedException, UnsupportedEncodingException, MQClientException {
        mockInvokeSync();
        mqClientAPI.updateControllerConfig(createProperties(), Collections.singletonList(defaultBrokerAddr), defaultTimeout);
    }

    @Test
    public void assertElectMaster() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        BrokerMemberGroup responseBody = new BrokerMemberGroup();
        setResponseBody(responseBody);
        GetMetaDataResponseHeader getMetaDataResponseHeader = new GetMetaDataResponseHeader();
        getMetaDataResponseHeader.setControllerLeaderAddress(defaultBrokerAddr);
        when(response.decodeCommandCustomHeader(GetMetaDataResponseHeader.class)).thenReturn(getMetaDataResponseHeader);
        ElectMasterResponseHeader responseHeader = new ElectMasterResponseHeader();
        when(response.decodeCommandCustomHeader(ElectMasterResponseHeader.class)).thenReturn(responseHeader);
        Pair<ElectMasterResponseHeader, BrokerMemberGroup> actual = mqClientAPI.electMaster(defaultBrokerAddr, clusterName, brokerName, MixAll.MASTER_ID);
        assertNotNull(actual);
        assertEquals(responseHeader, actual.getObject1());
        assertEquals(responseBody, actual.getObject2());
    }

    @Test
    public void testCleanControllerBrokerData() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        GetMetaDataResponseHeader responseHeader = new GetMetaDataResponseHeader();
        responseHeader.setControllerLeaderAddress(defaultBrokerAddr);
        setResponseHeader(responseHeader);
        mqClientAPI.cleanControllerBrokerData(defaultBrokerAddr, clusterName, brokerName, "", false);
    }

    @Test
    public void testCreateUser() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.createUser(defaultBrokerAddr, new UserInfo(), defaultTimeout);
    }

    @Test
    public void testUpdateUser() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.updateUser(defaultBrokerAddr, new UserInfo(), defaultTimeout);
    }

    @Test
    public void testDeleteUser() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.deleteUser(defaultBrokerAddr, "", defaultTimeout);
    }

    @Test
    public void assertGetUser() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        setResponseBody(createUserInfo());
        UserInfo actual = mqClientAPI.getUser(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals("username", actual.getUsername());
        assertEquals("password", actual.getPassword());
        assertEquals("userStatus", actual.getUserStatus());
        assertEquals("userType", actual.getUserType());
    }

    @Test
    public void assertListUser() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        setResponseBody(Collections.singletonList(createUserInfo()));
        List<UserInfo> actual = mqClientAPI.listUser(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals("username", actual.get(0).getUsername());
        assertEquals("password", actual.get(0).getPassword());
        assertEquals("userStatus", actual.get(0).getUserStatus());
        assertEquals("userType", actual.get(0).getUserType());
    }

    @Test
    public void testCreateAcl() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.createAcl(defaultBrokerAddr, new AclInfo(), defaultTimeout);
    }

    @Test
    public void testUpdateAcl() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.updateAcl(defaultBrokerAddr, new AclInfo(), defaultTimeout);
    }

    @Test
    public void testDeleteAcl() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        mqClientAPI.deleteAcl(defaultBrokerAddr, "", "", defaultTimeout);
    }

    @Test
    public void assertGetAcl() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        setResponseBody(createAclInfo());
        AclInfo actual = mqClientAPI.getAcl(defaultBrokerAddr, "", defaultTimeout);
        assertNotNull(actual);
        assertEquals("subject", actual.getSubject());
        assertEquals(1, actual.getPolicies().size());
    }

    @Test
    public void assertListAcl() throws RemotingException, InterruptedException, MQBrokerException {
        mockInvokeSync();
        setResponseBody(Collections.singletonList(createAclInfo()));
        List<AclInfo> actual = mqClientAPI.listAcl(defaultBrokerAddr, "", "", defaultTimeout);
        assertNotNull(actual);
        assertEquals("subject", actual.get(0).getSubject());
        assertEquals(1, actual.get(0).getPolicies().size());
    }

    @Test
    public void testRecallMessage() throws RemotingException, InterruptedException, MQBrokerException {
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setRecallHandle("handle");
        requestHeader.setBrokerName(brokerName);

        // success
        mockInvokeSync();
        String msgId = MessageClientIDSetter.createUniqID();
        RecallMessageResponseHeader responseHeader = new RecallMessageResponseHeader();
        responseHeader.setMsgId(msgId);
        setResponseHeader(responseHeader);
        String result = mqClientAPI.recallMessage(defaultBrokerAddr, requestHeader, defaultTimeout);
        assertEquals(msgId, result);

        // error
        when(response.getCode()).thenReturn(ResponseCode.SYSTEM_ERROR);
        when(response.getRemark()).thenReturn("error");
        MQBrokerException e = assertThrows(MQBrokerException.class, () -> {
            mqClientAPI.recallMessage(defaultBrokerAddr, requestHeader, defaultTimeout);
        });
        assertEquals(ResponseCode.SYSTEM_ERROR, e.getResponseCode());
        assertEquals("error", e.getErrorMessage());
        assertEquals(defaultBrokerAddr, e.getBrokerAddr());
    }

    @Test
    public void testRecallMessageAsync() throws RemotingException, InterruptedException {
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setRecallHandle("handle");
        requestHeader.setBrokerName(brokerName);
        String msgId = "msgId";
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback callback = mock.getArgument(3);
            RemotingCommand request = mock.getArgument(1);
            RemotingCommand response = RemotingCommand.createResponseCommand(RecallMessageResponseHeader.class);
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            RecallMessageResponseHeader responseHeader = (RecallMessageResponseHeader) response.readCustomHeader();
            responseHeader.setMsgId(msgId);
            ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
            responseFuture.setResponseCommand(response);
            callback.operationSucceed(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any(InvokeCallback.class));

        final CountDownLatch done = new CountDownLatch(1);
        mqClientAPI.recallMessageAsync(defaultBrokerAddr, requestHeader,
            defaultTimeout, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    RecallMessageResponseHeader responseHeader = (RecallMessageResponseHeader) response.readCustomHeader();
                    Assert.assertEquals(msgId, responseHeader.getMsgId());
                    done.countDown();
                }

                @Override
                public void operationFail(Throwable throwable) {
                }
            });
        done.await();
    }

    @Test
    public void testMQClientAPIImplWithoutObjectCreator() {
        MQClientAPIImpl clientAPI = new MQClientAPIImpl(
            new NettyClientConfig(),
            null,
            null,
            new ClientConfig(),
            null,
            null
        );
        RemotingClient remotingClient1 = clientAPI.getRemotingClient();
        Assert.assertTrue(remotingClient1 instanceof NettyRemotingClient);
    }

    @Test
    public void testMQClientAPIImplWithObjectCreator() {
        ObjectCreator<RemotingClient> clientObjectCreator = args -> new MockRemotingClientTest((NettyClientConfig) args[0]);
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        MQClientAPIImpl clientAPI = new MQClientAPIImpl(
            nettyClientConfig,
            null,
            null,
            new ClientConfig(),
            null,
            clientObjectCreator
        );
        RemotingClient remotingClient1 = clientAPI.getRemotingClient();
        Assert.assertTrue(remotingClient1 instanceof MockRemotingClientTest);
        MockRemotingClientTest remotingClientTest = (MockRemotingClientTest) remotingClient1;
        Assert.assertSame(remotingClientTest.getNettyClientConfig(), nettyClientConfig);
    }

    private static class MockRemotingClientTest extends NettyRemotingClient {
        public MockRemotingClientTest(NettyClientConfig nettyClientConfig) {
            super(nettyClientConfig);
        }

        public NettyClientConfig getNettyClientConfig() {
            return nettyClientConfig;
        }
    }

    private Properties createProperties() {
        Properties result = new Properties();
        result.put("key", "value");
        return result;
    }

    private AclInfo createAclInfo() {
        return AclInfo.of("subject", Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), "");
    }

    private UserInfo createUserInfo() {
        UserInfo result = new UserInfo();
        result.setUsername("username");
        result.setPassword("password");
        result.setUserStatus("userStatus");
        result.setUserType("userType");
        return result;
    }

    private void setResponseHeader(CommandCustomHeader responseHeader) throws RemotingCommandException {
        when(response.decodeCommandCustomHeader(any())).thenReturn(responseHeader);
    }

    private void setResponseBody(Object responseBody) {
        when(response.getBody()).thenReturn(RemotingSerializable.encode(responseBody));
    }

    private void mockInvokeSync() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        when(response.getCode()).thenReturn(ResponseCode.SUCCESS);
        when(response.getVersion()).thenReturn(1);
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        when(remotingClient.getNameServerAddressList()).thenReturn(Collections.singletonList(defaultNsAddr));
    }

    private void setTopAddressing() throws NoSuchFieldException, IllegalAccessException {
        TopAddressing topAddressing = mock(TopAddressing.class);
        setField(mqClientAPI, "topAddressing", topAddressing);
        when(topAddressing.fetchNSAddr()).thenReturn(defaultNsAddr);
    }

    private void setField(final Object target, final String fieldName, final Object newValue) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = target.getClass();
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, newValue);
    }
}
