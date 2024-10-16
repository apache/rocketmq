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
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.ChangeInvisibleTimeProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
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
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
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
    private ChangeInvisibleTimeProcessor changeInvisibleTimeProcessorMock;
    @Mock
    private AckMessageProcessor ackMessageProcessorMock;
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
        Mockito.when(brokerControllerMock.getChangeInvisibleTimeProcessor()).thenReturn(changeInvisibleTimeProcessorMock);
        Mockito.when(brokerControllerMock.getAckMessageProcessor()).thenReturn(ackMessageProcessorMock);
        Mockito.when(brokerControllerMock.getEndTransactionProcessor()).thenReturn(endTransactionProcessorMock);
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
