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

package org.apache.rocketmq.proxy.service.message;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.service.channel.ChannelManager;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@RunWith(MockitoJUnitRunner.class)
public class LocalMessageServiceTest extends InitConfigAndLoggerTest {
    private LocalMessageService localMessageService;
    @Mock
    private SendMessageProcessor sendMessageProcessorMock;
    @Mock
    private BrokerController brokerControllerMock;

    private ProxyContext proxyContext;

    private ChannelManager channelManager;

    private String topic = "topic";

    private int queueId = 0;

    private long queueOffset = 0L;

    private String transactionId = "transactionId";

    private String offsetMessageId = "offsetMessageId";

    @Before
    public void setUp() throws Throwable {
        super.before();
        ConfigurationManager.getProxyConfig().setNameSrvAddr("1.1.1.1");
        channelManager = new ChannelManager();
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(brokerControllerMock.getBrokerConfig()).thenReturn(new BrokerConfig());
        localMessageService = new LocalMessageService(brokerControllerMock, channelManager, null);
        proxyContext = ProxyContext.create().withVal(ContextVariable.REMOTE_ADDRESS, "0.0.0.1")
            .withVal(ContextVariable.LOCAL_ADDRESS, "0.0.0.2");
    }

    @Test
    public void testSendMessageWriteAndFlush() throws Exception {
        Message message = new Message(topic, "body".getBytes(StandardCharsets.UTF_8));
        MessageClientIDSetter.setUniqID(message);
        List<Message> messagesList = Collections.singletonList(message);
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.SEND_MESSAGE;
            boolean second = Arrays.equals(argument.getBody(), message.getBody());
            return first & second;
        }))).thenAnswer(invocation -> {
            SimpleChannelHandlerContext simpleChannelHandlerContext = invocation.getArgument(0);
            RemotingCommand request = invocation.getArgument(1);
            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            response.setOpaque(request.getOpaque());
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(message.getBody());
            SendMessageResponseHeader sendMessageResponseHeader = (SendMessageResponseHeader) response.readCustomHeader();
            sendMessageResponseHeader.setQueueId(queueId);
            sendMessageResponseHeader.setQueueOffset(queueOffset);
            sendMessageResponseHeader.setMsgId(offsetMessageId);
            sendMessageResponseHeader.setTransactionId(transactionId);
            simpleChannelHandlerContext.writeAndFlush(response);
            return null;
        });

        CompletableFuture<SendResult> future = localMessageService.sendMessage(proxyContext, null, messagesList, requestHeader, 1000L);
        SendResult sendResult = future.get();
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getMsgId()).isEqualTo(MessageClientIDSetter.getUniqID(message));
        assertThat(sendResult.getMessageQueue())
            .isEqualTo(new MessageQueue(topic, brokerControllerMock.getBrokerConfig().getBrokerName(), queueId));
        assertThat(sendResult.getQueueOffset()).isEqualTo(queueOffset);
        assertThat(sendResult.getTransactionId()).isEqualTo(transactionId);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo(offsetMessageId);
    }

    @Test
    public void testSendBatchMessageWriteAndFlush() throws Exception {
        Message message1 = new Message(topic, "body1".getBytes(StandardCharsets.UTF_8));
        Message message2 = new Message(topic, "body2".getBytes(StandardCharsets.UTF_8));
        MessageClientIDSetter.setUniqID(message1);
        MessageClientIDSetter.setUniqID(message2);
        List<Message> messagesList = Arrays.asList(message1, message2);
        MessageBatch msgBatch = MessageBatch.generateFromList(messagesList);
        MessageClientIDSetter.setUniqID(msgBatch);
        byte[] body = msgBatch.encode();
        msgBatch.setBody(body);
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.SEND_MESSAGE;
            boolean second = Arrays.equals(argument.getBody(), body);
            return first & second;
        }))).thenAnswer(invocation -> {
            SimpleChannelHandlerContext simpleChannelHandlerContext = invocation.getArgument(0);
            RemotingCommand request = invocation.getArgument(1);
            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            response.setOpaque(request.getOpaque());
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(body);
            SendMessageResponseHeader sendMessageResponseHeader = (SendMessageResponseHeader) response.readCustomHeader();
            sendMessageResponseHeader.setQueueId(queueId);
            sendMessageResponseHeader.setQueueOffset(queueOffset);
            sendMessageResponseHeader.setMsgId(offsetMessageId);
            sendMessageResponseHeader.setTransactionId(transactionId);
            simpleChannelHandlerContext.writeAndFlush(response);
            return null;
        });

        CompletableFuture<SendResult> future = localMessageService.sendMessage(proxyContext, null, messagesList, requestHeader, 1000L);
        SendResult sendResult = future.get();
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getMessageQueue())
            .isEqualTo(new MessageQueue(topic, brokerControllerMock.getBrokerConfig().getBrokerName(), queueId));
        assertThat(sendResult.getQueueOffset()).isEqualTo(queueOffset);
        assertThat(sendResult.getTransactionId()).isEqualTo(transactionId);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo(offsetMessageId);
    }

    @Test
    public void testSendMessageError() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(ResponseCode.SYSTEM_ERROR);
        Message message = new Message("topic", "body".getBytes(StandardCharsets.UTF_8));
        MessageClientIDSetter.setUniqID(message);
        List<Message> messagesList = Collections.singletonList(message);
        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
        sendMessageRequestHeader.setTopic(topic);
        sendMessageRequestHeader.setQueueId(queueId);

        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenReturn(response);

        CompletableFuture<SendResult> future = localMessageService.sendMessage(proxyContext, null, messagesList, sendMessageRequestHeader, 1000L);
        ExecutionException exception = catchThrowableOfType(future::get, ExecutionException.class);
        assertThat(exception.getCause()).isInstanceOf(ProxyException.class);
        assertThat(((ProxyException) exception.getCause()).getCode()).isEqualTo(ProxyExceptionCode.ILLEGAL_MESSAGE);
    }

    @Test
    public void testSendMessageWithException() throws Exception {
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenThrow(new RemotingCommandException("test"));
        Message message = new Message("topic", "body".getBytes(StandardCharsets.UTF_8));
        MessageClientIDSetter.setUniqID(message);
        List<Message> messagesList = Collections.singletonList(message);
        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
        CompletableFuture<SendResult> future = localMessageService.sendMessage(proxyContext, null, messagesList, sendMessageRequestHeader, 1000L);
        ExecutionException exception = catchThrowableOfType(future::get, ExecutionException.class);
        assertThat(exception.getCause()).isInstanceOf(RemotingCommandException.class);
    }
}