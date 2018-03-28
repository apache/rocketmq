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
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SendMessageProcessorTest {
    private SendMessageProcessor sendMessageProcessor;
    @Mock
    private ChannelHandlerContext handlerContext;
    @Spy
    private InternalLogger dlqLogger = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_DLQ_STATS_LOGGER_NAME);
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private MessageStore messageStore;

    private String topic = "FooBar";
    private String group = "FooBarGroup";

    @Before
    public void init() {
        brokerController.setMessageStore(messageStore);
        when(messageStore.now()).thenReturn(System.currentTimeMillis());
        Channel mockChannel = mock(Channel.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress(1024));
        when(handlerContext.channel()).thenReturn(mockChannel);
        when(messageStore.lookMessageByOffset(anyLong())).thenReturn(new MessageExt());
        sendMessageProcessor = new SendMessageProcessor(brokerController);
    }

    @Test
    public void testProcessRequest() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        assertPutResult(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_WithHook() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        List<SendMessageHook> sendMessageHookList = new ArrayList<>();
        final SendMessageContext[] sendMessageContext = new SendMessageContext[1];
        SendMessageHook sendMessageHook = new SendMessageHook() {
            @Override
            public String hookName() {
                return null;
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                sendMessageContext[0] = context;
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        };
        sendMessageHookList.add(sendMessageHook);
        sendMessageProcessor.registerSendMessageHook(sendMessageHookList);
        assertPutResult(ResponseCode.SUCCESS);
        System.out.println(sendMessageContext[0]);
        assertThat(sendMessageContext[0]).isNotNull();
        assertThat(sendMessageContext[0].getTopic()).isEqualTo(topic);
        assertThat(sendMessageContext[0].getProducerGroup()).isEqualTo(group);
    }

    @Test
    public void testProcessRequest_FlushTimeOut() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.FLUSH_DISK_TIMEOUT, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.FLUSH_DISK_TIMEOUT);
    }

    @Test
    public void testProcessRequest_MessageIllegal() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.MESSAGE_ILLEGAL);
    }

    @Test
    public void testProcessRequest_CreateMappedFileFailed() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testProcessRequest_FlushSlaveTimeout() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.FLUSH_SLAVE_TIMEOUT, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.FLUSH_SLAVE_TIMEOUT);
    }

    @Test
    public void testProcessRequest_PageCacheBusy() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testProcessRequest_PropertiesTooLong() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.MESSAGE_ILLEGAL);
    }

    @Test
    public void testProcessRequest_ServiceNotAvailable() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.SERVICE_NOT_AVAILABLE);
    }

    @Test
    public void testProcessRequest_SlaveNotAvailable() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.SLAVE_NOT_AVAILABLE, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR)));
        assertPutResult(ResponseCode.SLAVE_NOT_AVAILABLE);
    }

    @Test
    public void testProcessRequest_WithMsgBack() throws RemotingCommandException {
        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));
        final RemotingCommand request = createSendMsgBackCommand(RequestCode.CONSUMER_SEND_MSG_BACK);

        sendMessageProcessor = new SendMessageProcessor(brokerController);
        final RemotingCommand response = sendMessageProcessor.processRequest(handlerContext, request);
        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private RemotingCommand createSendMsgCommand(int requestCode) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(group);
        requestHeader.setTopic(topic);
        requestHeader.setDefaultTopic(MixAll.DEFAULT_TOPIC);
        requestHeader.setDefaultTopicQueueNums(3);
        requestHeader.setQueueId(1);
        requestHeader.setSysFlag(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(124);
        requestHeader.setReconsumeTimes(0);

        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand createSendMsgBackCommand(int requestCode) {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();

        requestHeader.setMaxReconsumeTimes(3);
        requestHeader.setDelayLevel(4);
        requestHeader.setGroup(group);
        requestHeader.setOffset(123L);

        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private void assertPutResult(int responseCode) throws RemotingCommandException {
        final RemotingCommand request = createSendMsgCommand(RequestCode.SEND_MESSAGE);
        final RemotingCommand[] response = new RemotingCommand[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                response[0] = invocation.getArgument(0);
                return null;
            }
        }).when(handlerContext).writeAndFlush(any(Object.class));
        RemotingCommand responseToReturn = sendMessageProcessor.processRequest(handlerContext, request);
        if (responseToReturn != null) {
            assertThat(response[0]).isNull();
            response[0] = responseToReturn;
        }
        assertThat(response[0].getCode()).isEqualTo(responseCode);
        assertThat(response[0].getOpaque()).isEqualTo(request.getOpaque());
    }

    @Test
    public void testDlqStatLog() throws RemotingCommandException, NoSuchFieldException, IllegalAccessException {
        String topic = "TopicA";
        String groupName = "TestDLQGroup";
        String msgId = "msgid4r3nufeu4gtbfy3gf3fy43g4";
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setRetryQueueNums(1);
        subscriptionGroupConfig.setConsumeEnable(true);
        subscriptionGroupConfig.setGroupName(groupName);
        subscriptionGroupConfig.setRetryQueueNums(5);

        final SubscriptionGroupManager subscriptionGroupManager = new SubscriptionGroupManager();
        subscriptionGroupManager.getSubscriptionGroupTable().put(subscriptionGroupConfig.getGroupName(),subscriptionGroupConfig);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return subscriptionGroupManager;
            }
        }).when(brokerController).getSubscriptionGroupManager();
        final StringBuilder resultCollector = new StringBuilder();

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topic);
        messageExt.setQueueId(0);
        messageExt.setBody("simple message".getBytes());
        messageExt.setCommitLogOffset(876867867L);
        messageExt.setReconsumeTimes(16);
        messageExt.setMsgId(msgId);
        messageExt.setStoreTimestamp(System.currentTimeMillis());
        messageExt.putUserProperty("test222","gggg");
        messageExt.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,msgId);
        messageExt.getProperties().put(MessageConst.PROPERTY_RETRY_TOPIC,topic);
        when(messageStore.lookMessageByOffset(anyLong())).thenReturn(messageExt);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                InnerLoggerFactory.MessageFormatter messageFormatter = new InnerLoggerFactory.MessageFormatter();
                String pattern = (String) arguments[0];
                Object[] objects = {arguments[1], arguments[2], arguments[3]};
                String message = messageFormatter.arrayFormat(pattern, objects).getMessage();
                resultCollector.append(message);
                return null;
            }
        }).when(dlqLogger).info(anyString(), new Object[]{Mockito.any()});

        Field dlqLoggerField = SendMessageProcessor.class.getDeclaredField("dlqLogger");
        dlqLoggerField.setAccessible(true);
        dlqLoggerField.set(sendMessageProcessor,dlqLogger);

        when(messageStore.putMessage(any(MessageExtBrokerInner.class))).thenReturn(new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK)));

        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setGroup(groupName);
        consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(3);
        consumerSendMsgBackRequestHeader.setOffset(876867867L);
        consumerSendMsgBackRequestHeader.setDelayLevel(10);
        consumerSendMsgBackRequestHeader.setOriginTopic("%RETRY%"+groupName);
        consumerSendMsgBackRequestHeader.setOriginMsgId(msgId);

        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, consumerSendMsgBackRequestHeader);
        requestCommand.addExtField("group",groupName);
        requestCommand.addExtField("maxReconsumeTimes","3");
        requestCommand.addExtField("offset","876867867");
        requestCommand.addExtField("delayLevel","10");
        requestCommand.addExtField("originTopic","%RETRY%"+groupName);
        requestCommand.addExtField("originMsgId",msgId);
        sendMessageProcessor.processRequest(null,requestCommand);
        String result = resultCollector.toString();
        assertThat(result).contains(msgId);
        assertThat(result).contains(topic);
        assertThat(result).contains(groupName);
    }
}