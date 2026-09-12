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

import io.netty.channel.Channel;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PopLiteMessageProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.broker.processor.RecallMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.service.channel.ChannelManager;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@RunWith(MockitoJUnitRunner.class)
public class LocalMessageServiceTest extends InitConfigTest {
    private LocalMessageService localMessageService;
    @Mock
    private SendMessageProcessor sendMessageProcessorMock;
    @Mock
    private EndTransactionProcessor endTransactionProcessorMock;
    @Mock
    private PopMessageProcessor popMessageProcessorMock;
    @Mock
    private PopLiteMessageProcessor popLiteMessageProcessorMock;
    @Mock
    private ChangeInvisibleTimeProcessor changeInvisibleTimeProcessorMock;
    @Mock
    private AckMessageProcessor ackMessageProcessorMock;
    @Mock
    private RecallMessageProcessor recallMessageProcessorMock;
    @Mock
    private BrokerController brokerControllerMock;

    private ProxyContext proxyContext;

    private ChannelManager channelManager;

    private String topic = "topic";

    private String brokerName = "brokerName";

    private int queueId = 0;

    private long queueOffset = 0L;

    private String transactionId = "transactionId";

    private String offsetMessageId = "offsetMessageId";

    @Before
    public void setUp() throws Throwable {
        super.before();
        ConfigurationManager.getProxyConfig().setNamesrvAddr("1.1.1.1");
        channelManager = new ChannelManager();
        Mockito.when(brokerControllerMock.getSendMessageProcessor()).thenReturn(sendMessageProcessorMock);
        Mockito.when(brokerControllerMock.getPopMessageProcessor()).thenReturn(popMessageProcessorMock);
        Mockito.when(brokerControllerMock.getPopLiteMessageProcessor()).thenReturn(popLiteMessageProcessorMock);
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessorMock);
        Mockito.when(brokerControllerMock.getAckMessageProcessor()).thenReturn(ackMessageProcessorMock);
        Mockito.when(brokerControllerMock.getEndTransactionProcessor()).thenReturn(endTransactionProcessorMock);
        Mockito.when(brokerControllerMock.getRecallMessageProcessor()).thenReturn(recallMessageProcessorMock);
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

        CompletableFuture<List<SendResult>> future = localMessageService.sendMessage(proxyContext, null, messagesList, requestHeader, 1000L);
        SendResult sendResult = future.get().get(0);
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

        CompletableFuture<List<SendResult>> future = localMessageService.sendMessage(proxyContext, null, messagesList, requestHeader, 1000L);
        SendResult sendResult = future.get().get(0);
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

        CompletableFuture<List<SendResult>> future = localMessageService.sendMessage(proxyContext, null, messagesList, sendMessageRequestHeader, 1000L);
        ExecutionException exception = catchThrowableOfType(future::get, ExecutionException.class);
        assertThat(exception.getCause()).isInstanceOf(ProxyException.class);
        assertThat(((ProxyException) exception.getCause()).getCode()).isEqualTo(ProxyExceptionCode.INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testSendMessageWithException() throws Exception {
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.any(RemotingCommand.class)))
            .thenThrow(new RemotingCommandException("test"));
        Message message = new Message("topic", "body".getBytes(StandardCharsets.UTF_8));
        MessageClientIDSetter.setUniqID(message);
        List<Message> messagesList = Collections.singletonList(message);
        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
        CompletableFuture<List<SendResult>> future = localMessageService.sendMessage(proxyContext, null, messagesList, sendMessageRequestHeader, 1000L);
        ExecutionException exception = catchThrowableOfType(future::get, ExecutionException.class);
        assertThat(exception.getCause()).isInstanceOf(RemotingCommandException.class);
    }

    @Test
    public void testSendMessageBack() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        Mockito.when(sendMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK;
            boolean second = argument.readCustomHeader() instanceof ConsumerSendMsgBackRequestHeader;
            return first && second;
        }))).thenReturn(remotingCommand);
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        CompletableFuture<RemotingCommand> future = localMessageService.sendMessageBack(proxyContext, null, null, requestHeader, 1000L);
        RemotingCommand response = future.get();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testEndTransaction() throws Exception {
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        localMessageService.endTransactionOneway(proxyContext, null, requestHeader, 1000L);
        Mockito.verify(endTransactionProcessorMock, Mockito.times(1)).processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.END_TRANSACTION;
            boolean second = argument.readCustomHeader() instanceof EndTransactionRequestHeader;
            return first && second;
        }));
    }

    @Test
    public void testPopLiteMessageWriteAndFlush() throws Exception {
        long popTime = System.currentTimeMillis();
        long invisibleTime = 3000L;
        List<MessageExt> messages = new ArrayList<>();
        MessageExt message1 = buildMessageExt(topic, 0, 100L);
        message1.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%$topic$lite");
        message1.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, "0");
        messages.add(message1);
        MessageExt message2 = buildMessageExt(topic, 0, 101L);
        message2.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%$topic$lite");
        message2.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, "1");
        messages.add(message2);
        byte[] body = ByteBuffer.allocate(MessageDecoder.encode(message1, false).length + MessageDecoder.encode(message2, false).length)
            .put(MessageDecoder.encode(message1, false)).put(MessageDecoder.encode(message2, false)).array();
        PopLiteMessageRequestHeader requestHeader = new PopLiteMessageRequestHeader();
        requestHeader.setInvisibleTime(invisibleTime);
        Mockito.when(popLiteMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument ->
            argument.getCode() == RequestCode.POP_LITE_MESSAGE
                && argument.readCustomHeader() instanceof PopLiteMessageRequestHeader))).thenAnswer(invocation -> {
                    SimpleChannelHandlerContext channelHandlerContext = invocation.getArgument(0);
                    RemotingCommand request = invocation.getArgument(1);
                    RemotingCommand response = RemotingCommand.createResponseCommand(PopLiteMessageResponseHeader.class);
                    response.setOpaque(request.getOpaque());
                    response.setCode(ResponseCode.SUCCESS);
                    response.setBody(body);
                    PopLiteMessageResponseHeader responseHeader = (PopLiteMessageResponseHeader) response.readCustomHeader();
                    responseHeader.setPopTime(popTime);
                    responseHeader.setInvisibleTime(invisibleTime);
                    responseHeader.setReviveQid(1);
                    channelHandlerContext.writeAndFlush(response);
                    return null;
                });

        PopResult result = localMessageService.popLiteMessage(proxyContext,
            new AddressableMessageQueue(new MessageQueue(topic, brokerName, queueId), ""), requestHeader, 1000L).get();
        assertThat(result.getPopStatus()).isEqualTo(PopStatus.FOUND);
        assertThat(result.getMsgFoundList()).hasSize(2);
        assertThat(result.getMsgFoundList().get(0).getQueueOffset()).isEqualTo(0L);
        assertThat(result.getMsgFoundList().get(1).getQueueOffset()).isEqualTo(1L);
        assertThat(result.getMsgFoundList().get(0).getBrokerName()).isEqualTo(brokerName);
    }

    @Test
    public void testPopLiteMessagePollingResponses() throws Exception {
        int[] codes = {ResponseCode.POLLING_FULL, ResponseCode.POLLING_TIMEOUT, ResponseCode.PULL_NOT_FOUND};
        PopStatus[] statuses = {PopStatus.POLLING_FULL, PopStatus.POLLING_NOT_FOUND, PopStatus.POLLING_NOT_FOUND};
        for (int i = 0; i < codes.length; i++) {
            PopResult result = invokeLiteResponse(RemotingCommand.createResponseCommand(codes[i], ""));
            assertThat(result.getPopStatus()).isEqualTo(statuses[i]);
            assertThat(result.getMsgFoundList()).isEmpty();
        }
    }

    @Test
    public void testPopLiteMessageEmptyBatch() throws Exception {
        PopResult result = invokeLiteResponse(buildLiteResponse(null));
        assertThat(result.getPopStatus()).isEqualTo(PopStatus.FOUND);
        assertThat(result.getMsgFoundList()).isEmpty();
    }

    @Test
    public void testPopLiteMessageMalformedDispatchProperties() throws Exception {
        String[][] properties = {
            {null, "7"}, {"%LMQ%$topic$lite", null},
            {"%LMQ%$topic$lite,%LMQ%$topic$other", "7,8"}, {"%LMQ%$topic$lite", "7,8"}
        };
        for (String[] property : properties) {
            MessageExt message = buildMessageExt(topic, 0, 100L);
            if (property[0] != null) {
                message.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, property[0]);
            }
            if (property[1] != null) {
                message.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, property[1]);
            }
            PopResult result = invokeLiteResponse(buildLiteResponse(null, message));
            assertThat(result.getMsgFoundList()).hasSize(1);
            assertThat(result.getMsgFoundList().get(0).getQueueOffset()).isEqualTo(100L);
            assertThat(result.getMsgFoundList().get(0).getProperty(MessageConst.PROPERTY_POP_CK)).isNull();
        }
    }

    @Test
    public void testPopLiteMessageMissingOrMismatchedOrderCounts() throws Exception {
        for (String counts : new String[] {null, "", "3", "3;4;5"}) {
            PopResult result = invokeLiteResponse(buildLiteResponse(counts, buildLiteMessage(7L), buildLiteMessage(8L)));
            assertThat(result.getMsgFoundList()).hasSize(2);
            for (MessageExt message : result.getMsgFoundList()) {
                assertThat(message.getReconsumeTimes()).isZero();
                assertThat(message.getProperty(MessageConst.PROPERTY_POP_CK)).isNotNull();
            }
        }
    }

    @Test
    public void testPopLiteMessageOrderCountsAndReceiptMetadata() throws Exception {
        PopResult result = invokeLiteResponse(buildLiteResponse("3;4", buildLiteMessage(7L), buildLiteMessage(8L)));
        for (int i = 0; i < 2; i++) {
            MessageExt message = result.getMsgFoundList().get(i);
            assertThat(message.getReconsumeTimes()).isEqualTo(i + 3);
            assertThat(message.getQueueOffset()).isEqualTo(i + 7L);
            assertThat(message.getBrokerName()).isEqualTo(brokerName);
            assertThat(message.getProperty(MessageConst.PROPERTY_FIRST_POP_TIME)).isEqualTo("123456");
            assertThat(message.getProperty(MessageConst.PROPERTY_POP_CK)).isEqualTo(
                ExtraInfoUtil.buildExtraInfo(0, 123456L, 3000L, 1, topic, brokerName, 0, i + 7L));
        }
    }

    @Test
    public void testPopLiteMessageBrokerError() throws Exception {
        ExecutionException exception = catchThrowableOfType(() -> invokeLiteResponse(
            RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "lite failure")), ExecutionException.class);
        assertThat(exception.getCause()).isInstanceOf(ProxyException.class);
        assertThat(((ProxyException) exception.getCause()).getCode()).isEqualTo(ProxyExceptionCode.INTERNAL_SERVER_ERROR);
        assertThat(exception.getCause()).hasMessage("lite failure");
    }

    @Test
    public void testPopLiteMessageProcessorException() throws Exception {
        RemotingCommandException failure = new RemotingCommandException("lite failure");
        Mockito.when(popLiteMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.any()))
            .thenThrow(failure);
        ExecutionException exception = catchThrowableOfType(() -> localMessageService.popLiteMessage(proxyContext,
            null, new PopLiteMessageRequestHeader(), 1000L).get(5, TimeUnit.SECONDS), ExecutionException.class);
        assertThat(exception.getCause()).isSameAs(failure);
    }

    private MessageExt buildLiteMessage(long offset) {
        MessageExt message = buildMessageExt(topic, 0, 100L);
        message.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%$topic$lite");
        message.getProperties().put(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, Long.toString(offset));
        return message;
    }

    private RemotingCommand buildLiteResponse(String orderCounts, MessageExt... messages) throws Exception {
        List<byte[]> encoded = new ArrayList<>();
        int size = 0;
        for (MessageExt message : messages) {
            byte[] bytes = MessageDecoder.encode(message, false);
            encoded.add(bytes);
            size += bytes.length;
        }
        ByteBuffer body = ByteBuffer.allocate(size);
        encoded.forEach(body::put);
        RemotingCommand response = RemotingCommand.createResponseCommand(PopLiteMessageResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(body.array());
        PopLiteMessageResponseHeader header = (PopLiteMessageResponseHeader) response.readCustomHeader();
        header.setPopTime(123456L);
        header.setInvisibleTime(3000L);
        header.setReviveQid(1);
        header.setOrderCountInfo(orderCounts);
        return response;
    }

    private PopResult invokeLiteResponse(RemotingCommand response) throws Exception {
        Mockito.when(popLiteMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.any()))
            .thenReturn(response);
        return localMessageService.popLiteMessage(proxyContext,
            new AddressableMessageQueue(new MessageQueue(topic, brokerName, queueId), ""),
            new PopLiteMessageRequestHeader(), 1000L).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testPopMessageWriteAndFlush() throws Exception {
        int reviveQueueId = 1;
        long popTime = System.currentTimeMillis();
        long invisibleTime = 3000L;
        long startOffset = 100L;
        long restNum = 0L;
        StringBuilder startOffsetStringBuilder = new StringBuilder();
        StringBuilder messageOffsetStringBuilder = new StringBuilder();
        List<MessageExt> messageExtList = new ArrayList<>();
        List<Long> messageOffsetList = new ArrayList<>();
        MessageExt message1 = buildMessageExt(topic, 0, startOffset);
        messageExtList.add(message1);
        messageOffsetList.add(startOffset);
        byte[] body1 = MessageDecoder.encode(message1, false);
        MessageExt message2 = buildMessageExt(topic, 0, startOffset + 1);
        messageExtList.add(message2);
        messageOffsetList.add(startOffset + 1);
        ExtraInfoUtil.buildStartOffsetInfo(startOffsetStringBuilder, topic, queueId, startOffset);
        ExtraInfoUtil.buildMsgOffsetInfo(messageOffsetStringBuilder, topic, queueId, messageOffsetList);
        byte[] body2 = MessageDecoder.encode(message2, false);
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(body1);
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(body2);
        ByteBuffer b3 = ByteBuffer.allocate(byteBuffer1.limit() + byteBuffer2.limit());
        b3.put(byteBuffer1);
        b3.put(byteBuffer2);
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setInvisibleTime(invisibleTime);
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.POP_MESSAGE;
            boolean second = argument.readCustomHeader() instanceof PopMessageRequestHeader;
            return first && second;
        }))).thenAnswer(invocation -> {
            SimpleChannelHandlerContext simpleChannelHandlerContext = invocation.getArgument(0);
            RemotingCommand request = invocation.getArgument(1);
            RemotingCommand response = RemotingCommand.createResponseCommand(PopMessageResponseHeader.class);
            response.setOpaque(request.getOpaque());
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(b3.array());
            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.readCustomHeader();
            responseHeader.setStartOffsetInfo(startOffsetStringBuilder.toString());
            responseHeader.setMsgOffsetInfo(messageOffsetStringBuilder.toString());
            responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
            responseHeader.setPopTime(popTime);
            responseHeader.setRestNum(restNum);
            responseHeader.setReviveQid(reviveQueueId);
            simpleChannelHandlerContext.writeAndFlush(response);
            return null;
        });
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
        CompletableFuture<PopResult> future = localMessageService.popMessage(proxyContext, new AddressableMessageQueue(messageQueue, ""), requestHeader, 1000L);
        PopResult popResult = future.get();
        assertThat(popResult.getPopTime()).isEqualTo(popTime);
        assertThat(popResult.getInvisibleTime()).isEqualTo(invisibleTime);
        assertThat(popResult.getPopStatus()).isEqualTo(PopStatus.FOUND);
        assertThat(popResult.getRestNum()).isEqualTo(restNum);
        assertThat(popResult.getMsgFoundList().size()).isEqualTo(messageExtList.size());
        for (int i = 0; i < popResult.getMsgFoundList().size(); i++) {
            assertMessageExt(popResult.getMsgFoundList().get(i), messageExtList.get(i));
            assertThat(popResult.getMsgFoundList().get(i).getBrokerName()).isEqualTo(brokerName);
        }
    }

    @Test
    public void testPopMessagePollingTimeout() throws Exception {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.POLLING_TIMEOUT, "");
        Mockito.when(popMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.POP_MESSAGE;
            boolean second = argument.readCustomHeader() instanceof PopMessageRequestHeader;
            return first && second;
        }))).thenReturn(remotingCommand);
        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        CompletableFuture<PopResult> future = localMessageService.popMessage(proxyContext, null, requestHeader, 1000L);
        PopResult popResult = future.get();
        assertThat(popResult.getPopStatus()).isEqualTo(PopStatus.POLLING_NOT_FOUND);
    }

    @Test
    public void testChangeInvisibleTime() throws Exception {
        String messageId = "messageId";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 3000L;
        int reviveQueueId = 1;
        ReceiptHandle handle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(popTime)
            .invisibleTime(invisibleTime)
            .reviveQueueId(reviveQueueId)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(queueOffset)
            .build();
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
        remotingCommand.setCode(ResponseCode.SUCCESS);
        remotingCommand.setRemark("");
        long newPopTime = System.currentTimeMillis();
        long newInvisibleTime = 5000L;
        int newReviveQueueId = 2;
        ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) remotingCommand.readCustomHeader();
        responseHeader.setReviveQid(newReviveQueueId);
        responseHeader.setInvisibleTime(newInvisibleTime);
        responseHeader.setPopTime(newPopTime);
        Mockito.when(changeInvisibleTimeProcessorMock.processRequestAsync(Mockito.any(Channel.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.CHANGE_MESSAGE_INVISIBLETIME;
            boolean second = argument.readCustomHeader() instanceof ChangeInvisibleTimeRequestHeader;
            return first && second;
        }), Mockito.any(Boolean.class))).thenReturn(CompletableFuture.completedFuture(remotingCommand));
        ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
        CompletableFuture<AckResult> future = localMessageService.changeInvisibleTime(proxyContext, handle, messageId,
            requestHeader, 1000L);
        AckResult ackResult = future.get();
        assertThat(ackResult.getStatus()).isEqualTo(AckStatus.OK);
        assertThat(ackResult.getPopTime()).isEqualTo(newPopTime);
        assertThat(ackResult.getExtraInfo()).isEqualTo(ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(newPopTime)
            .invisibleTime(newInvisibleTime)
            .reviveQueueId(newReviveQueueId)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(queueOffset)
            .build()
            .encode());
    }

    @Test
    public void testAckMessage() throws Exception {
        String messageId = "messageId";
        long popTime = System.currentTimeMillis();
        long invisibleTime = 3000L;
        int reviveQueueId = 1;
        ReceiptHandle handle = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(popTime)
            .invisibleTime(invisibleTime)
            .reviveQueueId(reviveQueueId)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(queueOffset)
            .build();
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        Mockito.when(ackMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class), Mockito.argThat(argument -> {
            boolean first = argument.getCode() == RequestCode.ACK_MESSAGE;
            boolean second = argument.readCustomHeader() instanceof AckMessageRequestHeader;
            return first && second;
        }))).thenReturn(remotingCommand);
        AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
        CompletableFuture<AckResult> future = localMessageService.ackMessage(proxyContext, handle, messageId,
            requestHeader, 1000L);
        AckResult ackResult = future.get();
        assertThat(ackResult.getStatus()).isEqualTo(AckStatus.OK);
    }

    @Test
    public void testRecallMessage_success() throws Exception {
        RecallMessageResponseHeader responseHeader = new RecallMessageResponseHeader();
        responseHeader.setMsgId("msgId");
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SUCCESS, responseHeader);
        Mockito.when(recallMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class),
            Mockito.any())).thenReturn(response);
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        String msgId = localMessageService.recallMessage(proxyContext, "brokerName", requestHeader, 1000L).join();
        assertThat(msgId).isEqualTo("msgId");
    }

    @Test
    public void testRecallMessage_fail() throws Exception {
        RecallMessageResponseHeader responseHeader = new RecallMessageResponseHeader();
        RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(ResponseCode.SLAVE_NOT_AVAILABLE, responseHeader);
        Mockito.when(recallMessageProcessorMock.processRequest(Mockito.any(SimpleChannelHandlerContext.class),
            Mockito.any())).thenReturn(response);
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        CompletionException exception = Assert.assertThrows(CompletionException.class, () -> {
            localMessageService.recallMessage(proxyContext, "brokerName", requestHeader, 1000L).join();
        });
        Assert.assertTrue(exception.getCause() instanceof ProxyException);
    }

    private MessageExt buildMessageExt(String topic, int queueId, long queueOffset) {
        MessageExt message1 = new MessageExt();
        message1.setTopic(topic);
        message1.setBody("body".getBytes(StandardCharsets.UTF_8));
        message1.setFlag(0);
        message1.setQueueId(queueId);
        message1.setQueueOffset(queueOffset);
        message1.setCommitLogOffset(1000L);
        message1.setSysFlag(0);
        message1.setBornTimestamp(0L);
        InetSocketAddress inetSocketAddress = new InetSocketAddress("127.0.0.1", 80);
        message1.setBornHost(inetSocketAddress);
        message1.setStoreHost(inetSocketAddress);
        message1.setReconsumeTimes(0);
        message1.setPreparedTransactionOffset(0L);
        message1.putUserProperty("K", "V");
        return message1;
    }

    private void assertMessageExt(MessageExt messageExt1, MessageExt messageExt2) {
        assertThat(messageExt1.getBody()).isEqualTo(messageExt2.getBody());
        assertThat(messageExt1.getTopic()).isEqualTo(messageExt2.getTopic());
        assertThat(messageExt1.getQueueId()).isEqualTo(messageExt2.getQueueId());
        assertThat(messageExt1.getQueueOffset()).isEqualTo(messageExt2.getQueueOffset());
    }
}
