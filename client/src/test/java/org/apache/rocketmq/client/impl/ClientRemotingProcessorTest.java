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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.rocketmq.common.message.MessageDecoder.NAME_VALUE_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientRemotingProcessorTest {

    @Mock
    private MQClientInstance mQClientFactory;

    private ClientRemotingProcessor processor;

    private final String defaultTopic = "defaultTopic";

    private final String defaultBroker = "defaultBroker";

    private final String defaultGroup = "defaultGroup";

    @Before
    public void init() throws RemotingException, InterruptedException, MQClientException {
        processor = new ClientRemotingProcessor(mQClientFactory);
        ClientConfig clientConfig = mock(ClientConfig.class);
        when(clientConfig.getNamespace()).thenReturn("namespace");
        when(mQClientFactory.getClientConfig()).thenReturn(clientConfig);
        MQProducerInner producerInner = mock(MQProducerInner.class);
        when(mQClientFactory.selectProducer(defaultGroup)).thenReturn(producerInner);
    }

    @Test
    public void testCheckTransactionState() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.CHECK_TRANSACTION_STATE);
        when(request.getBody()).thenReturn(getMessageResult());
        CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        when(request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class)).thenReturn(requestHeader);
        assertNull(processor.processRequest(ctx, request));
    }

    @Test
    public void testNotifyConsumerIdsChanged() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED);
        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        when(request.decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class)).thenReturn(requestHeader);
        assertNull(processor.processRequest(ctx, request));
    }

    @Test
    public void testResetOffset() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.RESET_CONSUMER_CLIENT_OFFSET);
        ResetOffsetBody offsetBody = new ResetOffsetBody();
        when(request.getBody()).thenReturn(RemotingSerializable.encode(offsetBody));
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        when(request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class)).thenReturn(requestHeader);
        assertNull(processor.processRequest(ctx, request));
    }

    @Test
    public void testGetConsumeStatus() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT);
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        when(request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class)).thenReturn(requestHeader);
        assertNotNull(processor.processRequest(ctx, request));
    }

    @Test
    public void testGetConsumerRunningInfo() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.GET_CONSUMER_RUNNING_INFO);
        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("jstack");
        when(mQClientFactory.consumerRunningInfo(anyString())).thenReturn(consumerRunningInfo);
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setJstackEnable(true);
        requestHeader.setConsumerGroup(defaultGroup);
        when(request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class)).thenReturn(requestHeader);
        RemotingCommand command = processor.processRequest(ctx, request);
        assertNotNull(command);
        assertEquals(ResponseCode.SUCCESS, command.getCode());
    }

    @Test
    public void testConsumeMessageDirectly() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.CONSUME_MESSAGE_DIRECTLY);
        when(request.getBody()).thenReturn(getMessageResult());
        ConsumeMessageDirectlyResult directlyResult = mock(ConsumeMessageDirectlyResult.class);
        when(mQClientFactory.consumeMessageDirectly(any(MessageExt.class), anyString(), anyString())).thenReturn(directlyResult);
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setConsumerGroup(defaultGroup);
        requestHeader.setBrokerName(defaultBroker);
        when(request.decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class)).thenReturn(requestHeader);
        RemotingCommand command = processor.processRequest(ctx, request);
        assertNotNull(command);
        assertEquals(ResponseCode.SUCCESS, command.getCode());
    }

    @Test
    public void testReceiveReplyMessage() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT);
        when(request.getBody()).thenReturn(getMessageResult());
        when(request.decodeCommandCustomHeader(ReplyMessageRequestHeader.class)).thenReturn(createReplyMessageRequestHeader());
        when(request.getBody()).thenReturn(new byte[1]);
        RemotingCommand command = processor.processRequest(ctx, request);
        assertNotNull(command);
        assertEquals(ResponseCode.SUCCESS, command.getCode());
    }

    private ReplyMessageRequestHeader createReplyMessageRequestHeader() {
        ReplyMessageRequestHeader result = new ReplyMessageRequestHeader();
        result.setTopic(defaultTopic);
        result.setQueueId(0);
        result.setStoreTimestamp(System.currentTimeMillis());
        result.setBornTimestamp(System.currentTimeMillis());
        result.setReconsumeTimes(1);
        result.setBornHost("127.0.0.1:12911");
        result.setStoreHost("127.0.0.1:10911");
        result.setSysFlag(1);
        result.setFlag(1);
        result.setProperties("CORRELATION_ID" + NAME_VALUE_SEPARATOR + "1");
        return result;
    }

    private byte[] getMessageResult() throws Exception {
        byte[] bytes = MessageDecoder.encode(createMessageExt(), false);
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);
        return byteBuffer.array();
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(defaultTopic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, defaultGroup);
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        result.setKeys("keys");
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setStoreHost(storeHost);
        result.setBornHost(bornHost);
        return result;
    }
}
