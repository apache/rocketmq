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

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerClusterAclConfigResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerClusterAclConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

@RunWith(MockitoJUnitRunner.class)
public class MQClientAPIImplTest {
    private MQClientAPIImpl mqClientAPI = new MQClientAPIImpl(new NettyClientConfig(), null, null, new ClientConfig());
    @Mock
    private RemotingClient remotingClient;
    @Mock
    private DefaultMQProducerImpl defaultMQProducerImpl;

    private String brokerAddr = "127.0.0.1";
    private String brokerName = "DefaultBroker";
    private String clusterName = "DefaultCluster";
    private static String group = "FooBarGroup";
    private static String topic = "FooBar";
    private Message msg = new Message("FooBar", new byte[] {});
    private static String clientId = "127.0.0.2@UnitTest";

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
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createSendMessageSuccessResponse(request);
            }
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
    public void testSendMessageSync_WithException() throws InterruptedException, RemotingException, MQBrokerException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                response.setRemark("Broker is broken.");
                return response;
            }
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

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                InvokeCallback callback = mock.getArgument(3);
                RemotingCommand request = mock.getArgument(1);
                ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
                responseFuture.setResponseCommand(createSendMessageSuccessResponse(request));
                callback.operationSuccess(responseFuture.getResponseCommand());
                return null;
            }
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
    public void testCreatePlainAccessConfig_Success() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createSuccessResponse4UpdateAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        PlainAccessConfig config = createUpdateAclConfig();

        try {
            mqClientAPI.createPlainAccessConfig(brokerAddr, config, 3 * 1000);
        } catch (MQClientException ex) {

        }
    }

    @Test
    public void testCreatePlainAccessConfig_Exception() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createErrorResponse4UpdateAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        PlainAccessConfig config = createUpdateAclConfig();
        try {
            mqClientAPI.createPlainAccessConfig(brokerAddr, config, 3 * 1000);
        } catch (MQClientException ex) {
            assertThat(ex.getResponseCode()).isEqualTo(209);
            assertThat(ex.getErrorMessage()).isEqualTo("corresponding to accessConfig has been updated failed");
        }
    }

    @Test
    public void testDeleteAccessConfig_Success() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createSuccessResponse4DeleteAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        String accessKey = "1234567";
        try {
            mqClientAPI.deleteAccessConfig(brokerAddr, accessKey, 3 * 1000);
        } catch (MQClientException ex) {

        }
    }

    @Test
    public void testDeleteAccessConfig_Exception() throws InterruptedException, RemotingException, MQBrokerException {

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createErrorResponse4DeleteAclConfig(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        try {
            mqClientAPI.deleteAccessConfig(brokerAddr, "11111", 3 * 1000);
        } catch (MQClientException ex) {
            assertThat(ex.getResponseCode()).isEqualTo(210);
            assertThat(ex.getErrorMessage()).isEqualTo("corresponding to accessConfig has been deleted failed");
        }
    }

    @Test
    public void testResumeCheckHalfMessage_WithException() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                response.setRemark("Put message back to RMQ_SYS_TRANS_HALF_TOPIC failed.");
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        boolean result = mqClientAPI.resumeCheckHalfMessage(brokerAddr, "test", 3000);
        assertThat(result).isEqualTo(false);
    }

    @Test
    public void testResumeCheckHalfMessage_Success() throws InterruptedException, RemotingException, MQBrokerException, MQClientException {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                RemotingCommand request = mock.getArgument(1);
                return createResumeSuccessResponse(request);
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        boolean result = mqClientAPI.resumeCheckHalfMessage(brokerAddr, "test", 3000);

        assertThat(result).isEqualTo(true);
    }

    @Test
    public void testSendMessageTypeofReply() throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                InvokeCallback callback = mock.getArgument(3);
                RemotingCommand request = mock.getArgument(1);
                ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
                responseFuture.setResponseCommand(createSendMessageSuccessResponse(request));
                callback.operationSuccess(responseFuture.getResponseCommand());
                return null;
            }
        }).when(remotingClient).invokeAsync(Matchers.anyString(), Matchers.any(RemotingCommand.class), Matchers.anyLong(), Matchers.any(InvokeCallback.class));
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
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                RemotingCommand response = RemotingCommand.createResponseCommand(null);
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                QueryAssignmentResponseBody b = new QueryAssignmentResponseBody();
                b.setMessageQueueAssignments(Collections.singleton(new MessageQueueAssignment()));
                response.setBody(b.encode());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());
        Set<MessageQueueAssignment> assignments = mqClientAPI.queryAssignment(brokerAddr, topic, group, clientId, null, MessageModel.CLUSTERING, 10 * 1000);
        assertThat(assignments).size().isEqualTo(1);
    }

    @Test
    public void testPopMessageAsync_Success() throws Exception {
        final long popTime = System.currentTimeMillis();
        final int invisibleTime = 10 * 1000;
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock mock) throws Throwable {
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
                ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, false, 0, 0L);
                responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
                StringBuilder msgOffsetInfo = new StringBuilder(64);
                ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, false, 0, Collections.singletonList(0L));
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
                callback.operationSuccess(responseFuture.getResponseCommand());
                return null;
            }
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
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock mock) throws Throwable {
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
                ExtraInfoUtil.buildStartOffsetInfo(startOffsetInfo, false, 0, 0L);
                responseHeader.setStartOffsetInfo(startOffsetInfo.toString());
                StringBuilder msgOffsetInfo = new StringBuilder(64);
                ExtraInfoUtil.buildMsgOffsetInfo(msgOffsetInfo, false, 0, Collections.singletonList(0L));
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
                callback.operationSuccess(responseFuture.getResponseCommand());
                return null;
            }
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
    public void testAckMessageAsync_Success() throws Exception {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock mock) throws Throwable {
                InvokeCallback callback = mock.getArgument(3);
                RemotingCommand request = mock.getArgument(1);
                ResponseFuture responseFuture = new ResponseFuture(null, request.getOpaque(), 3 * 1000, null, null);
                RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                response.setOpaque(request.getOpaque());
                response.setCode(ResponseCode.SUCCESS);
                responseFuture.setResponseCommand(response);
                callback.operationSuccess(responseFuture.getResponseCommand());
                return null;
            }
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
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock mock) throws Throwable {
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
                callback.operationSuccess(responseFuture.getResponseCommand());
                return null;
            }
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
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                RemotingCommand response = RemotingCommand.createResponseCommand(null);
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.setMessageRequestMode(brokerAddr, topic, group, MessageRequestMode.POP, 8, 10 * 1000L);
    }

    @Test
    public void testCreateSubscriptionGroup_Success() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                RemotingCommand response = RemotingCommand.createResponseCommand(null);
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.createSubscriptionGroup(brokerAddr, new SubscriptionGroupConfig(), 10000);
    }

    @Test
    public void testCreateTopic_Success() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                RemotingCommand response = RemotingCommand.createResponseCommand(null);
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.createTopic(brokerAddr, topic, new TopicConfig(), 10000);
    }

    @Test
    public void testGetBrokerClusterConfig() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerClusterAclConfigResponseHeader.class);
                GetBrokerClusterAclConfigResponseBody body = new GetBrokerClusterAclConfigResponseBody();
                body.setGlobalWhiteAddrs(Collections.singletonList("1.1.1.1"));
                body.setPlainAccessConfigs(Collections.singletonList(new PlainAccessConfig()));
                response.setBody(body.encode());
                response.makeCustomHeaderToNet();
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        AclConfig aclConfig = mqClientAPI.getBrokerClusterConfig(brokerAddr, 10000);
        assertThat(aclConfig.getPlainAccessConfigs()).size().isGreaterThan(0);
        assertThat(aclConfig.getGlobalWhiteAddrs()).size().isGreaterThan(0);
    }

    @Test
    public void testViewMessage() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) throws Exception {
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
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        MessageExt messageExt = mqClientAPI.viewMessage(brokerAddr, 100L, 10000);
        assertThat(messageExt.getTopic()).isEqualTo(topic);
    }

    @Test
    public void testSearchOffset() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
                final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
                responseHeader.setOffset(100L);
                response.makeCustomHeaderToNet();
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long offset = mqClientAPI.searchOffset(brokerAddr, topic, 0, System.currentTimeMillis() - 1000, 10000);
        assertThat(offset).isEqualTo(100L);
    }

    @Test
    public void testGetMaxOffset() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
                final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
                responseHeader.setOffset(100L);
                response.makeCustomHeaderToNet();
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long offset = mqClientAPI.getMaxOffset(brokerAddr, new MessageQueue(topic, brokerName, 0), 10000);
        assertThat(offset).isEqualTo(100L);
    }

    @Test
    public void testGetMinOffset() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                final RemotingCommand response = RemotingCommand.createResponseCommand(GetMinOffsetResponseHeader.class);
                final GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();
                responseHeader.setOffset(100L);
                response.makeCustomHeaderToNet();
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long offset = mqClientAPI.getMinOffset(brokerAddr, new MessageQueue(topic, brokerName, 0), 10000);
        assertThat(offset).isEqualTo(100L);
    }

    @Test
    public void testGetEarliestMsgStoretime() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                final RemotingCommand response = RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
                final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
                responseHeader.setTimestamp(100L);
                response.makeCustomHeaderToNet();
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long t = mqClientAPI.getEarliestMsgStoretime(brokerAddr, new MessageQueue(topic, brokerName, 0), 10000);
        assertThat(t).isEqualTo(100L);
    }

    @Test
    public void testQueryConsumerOffset() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
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
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        long t = mqClientAPI.queryConsumerOffset(brokerAddr, new QueryConsumerOffsetRequestHeader(), 1000);
        assertThat(t).isEqualTo(100L);
    }

    @Test
    public void testUpdateConsumerOffset() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
                RemotingCommand request = mock.getArgument(1);

                final RemotingCommand response =
                    RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
                response.makeCustomHeaderToNet();
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                return response;
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        mqClientAPI.updateConsumerOffset(brokerAddr, new UpdateConsumerOffsetRequestHeader(), 1000);
    }

    @Test
    public void testGetConsumerIdListByGroup() throws Exception {
        doAnswer(new Answer<RemotingCommand>() {
            @Override
            public RemotingCommand answer(InvocationOnMock mock) {
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
            }
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

    private RemotingCommand createErrorResponse4UpdateAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark("corresponding to accessConfig has been updated failed");

        return response;
    }

    private RemotingCommand createErrorResponse4DeleteAclConfig(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.DELETE_ACL_CONFIG_FAILED);
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setRemark("corresponding to accessConfig has been deleted failed");

        return response;
    }

    private PlainAccessConfig createUpdateAclConfig() {

        PlainAccessConfig config = new PlainAccessConfig();
        config.setAccessKey("Rocketmq111");
        config.setSecretKey("123456789");
        config.setAdmin(true);
        config.setWhiteRemoteAddress("127.0.0.1");
        config.setDefaultTopicPerm("DENY");
        config.setDefaultGroupPerm("SUB");
        return config;
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
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
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
            }
        }).when(remotingClient).invokeSync(anyString(), any(RemotingCommand.class), anyLong());

        int topicCnt = mqClientAPI.addWritePermOfBroker("127.0.0.1", "default-broker", 1000);
        assertThat(topicCnt).isEqualTo(7);
    }
}
