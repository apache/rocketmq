/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.mqclient;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.mqclient.DoNothingClientRemotingProcessor;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class MQClientAPIExtTest {

    private static final String BROKER_ADDR = "127.0.0.1:10911";
    private static final String BROKER_NAME = "brokerName";
    private static final long TIMEOUT = 3000;
    private static final String CONSUMER_GROUP = "group";
    private static final String TOPIC = "topic";

    @Spy
    private final MQClientAPIExt mqClientAPI = new MQClientAPIExt(new ClientConfig(), new NettyClientConfig(), new DoNothingClientRemotingProcessor(null), null);
    @Mock
    private RemotingClient remotingClient;

    @Before
    public void init() throws Exception {
        Field field = MQClientAPIImpl.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        field.set(mqClientAPI, remotingClient);
    }

    @Test
    public void testSendHeartbeatAsync() throws Exception {
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            responseFuture.putResponse(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, ""));
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        assertNotNull(mqClientAPI.sendHeartbeatAsync(BROKER_ADDR, new HeartbeatData(), TIMEOUT).get());
    }

    @Test
    public void testSendMessageAsync() throws Exception {
        AtomicReference<String> msgIdRef = new AtomicReference<>();
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            SendMessageResponseHeader sendMessageResponseHeader = (SendMessageResponseHeader) response.readCustomHeader();
            sendMessageResponseHeader.setMsgId(msgIdRef.get());
            sendMessageResponseHeader.setQueueId(0);
            sendMessageResponseHeader.setQueueOffset(1L);
            response.setCode(ResponseCode.SUCCESS);
            response.makeCustomHeaderToNet();
            responseFuture.putResponse(response);
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        MessageExt messageExt = createMessage();
        msgIdRef.set(MessageClientIDSetter.getUniqID(messageExt));

        SendResult sendResult = mqClientAPI.sendMessageAsync(BROKER_ADDR, BROKER_NAME, messageExt, new SendMessageRequestHeader(), TIMEOUT)
            .get();
        assertNotNull(sendResult);
        assertEquals(msgIdRef.get(), sendResult.getMsgId());
        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }

    @Test
    public void testSendMessageListAsync() throws Exception {
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
            SendMessageResponseHeader sendMessageResponseHeader = (SendMessageResponseHeader) response.readCustomHeader();
            sendMessageResponseHeader.setMsgId("");
            sendMessageResponseHeader.setQueueId(0);
            sendMessageResponseHeader.setQueueOffset(1L);
            response.setCode(ResponseCode.SUCCESS);
            response.makeCustomHeaderToNet();
            responseFuture.putResponse(response);
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        List<MessageExt> messageExtList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            MessageExt messageExt = createMessage();
            sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(messageExt));
            messageExtList.add(messageExt);
        }

        SendResult sendResult = mqClientAPI.sendMessageAsync(BROKER_ADDR, BROKER_NAME, messageExtList, new SendMessageRequestHeader(), TIMEOUT)
            .get();
        assertNotNull(sendResult);
        assertEquals(sb.toString(), sendResult.getMsgId());
        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
    }

    @Test
    public void testSendMessageBackAsync() throws Exception {
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            responseFuture.putResponse(RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, ""));
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        RemotingCommand remotingCommand = mqClientAPI.sendMessageBackAsync(BROKER_ADDR, new ConsumerSendMsgBackRequestHeader(), TIMEOUT)
            .get();
        assertNotNull(remotingCommand);
        assertEquals(ResponseCode.SUCCESS, remotingCommand.getCode());
    }

    @Test
    public void testPopMessageAsync() throws Exception {
        PopResult popResult = new PopResult(PopStatus.POLLING_NOT_FOUND, null);
        doAnswer((Answer<Void>) mock -> {
            PopCallback popCallback = mock.getArgument(4);
            popCallback.onSuccess(popResult);
            return null;
        }).when(mqClientAPI).popMessageAsync(anyString(), anyString(), any(), anyLong(), any());

        assertSame(popResult, mqClientAPI.popMessageAsync(BROKER_ADDR, BROKER_NAME, new PopMessageRequestHeader(), TIMEOUT).get());
    }

    @Test
    public void testAckMessageAsync() throws Exception {
        AckResult ackResult = new AckResult();
        doAnswer((Answer<Void>) mock -> {
            AckCallback ackCallback = mock.getArgument(2);
            ackCallback.onSuccess(ackResult);
            return null;
        }).when(mqClientAPI).ackMessageAsync(anyString(), anyLong(), any(AckCallback.class), any());

        assertSame(ackResult, mqClientAPI.ackMessageAsync(BROKER_ADDR, new AckMessageRequestHeader(), TIMEOUT).get());
    }

    @Test
    public void testBatchAckMessageAsync() throws Exception {
        AckResult ackResult = new AckResult();
        doAnswer((Answer<Void>) mock -> {
            AckCallback ackCallback = mock.getArgument(2);
            ackCallback.onSuccess(ackResult);
            return null;
        }).when(mqClientAPI).batchAckMessageAsync(anyString(), anyLong(), any(AckCallback.class), any());

        assertSame(ackResult, mqClientAPI.batchAckMessageAsync(BROKER_ADDR, TOPIC, CONSUMER_GROUP, new ArrayList<>(), TIMEOUT).get());
    }

    @Test
    public void testChangeInvisibleTimeAsync() throws Exception {
        AckResult ackResult = new AckResult();
        doAnswer((Answer<Void>) mock -> {
            AckCallback ackCallback = mock.getArgument(4);
            ackCallback.onSuccess(ackResult);
            return null;
        }).when(mqClientAPI).changeInvisibleTimeAsync(anyString(), anyString(), any(), anyLong(), any(AckCallback.class));

        assertSame(ackResult, mqClientAPI.changeInvisibleTimeAsync(BROKER_ADDR, BROKER_NAME, new ChangeInvisibleTimeRequestHeader(), TIMEOUT).get());
    }

    @Test
    public void testPullMessageAsync() throws Exception {
        MessageExt msg1 = createMessage();
        byte[] msg1Byte = MessageDecoder.encode(msg1, false);
        MessageExt msg2 = createMessage();
        byte[] msg2Byte = MessageDecoder.encode(msg2, false);

        ByteBuffer byteBuffer = ByteBuffer.allocate(msg1Byte.length + msg2Byte.length);
        byteBuffer.put(msg1Byte);
        byteBuffer.put(msg2Byte);

        PullResultExt pullResultExt = new PullResultExt(PullStatus.FOUND, 0, 0, 1, null, 0,
            byteBuffer.array());
        doAnswer((Answer<Void>) mock -> {
            PullCallback pullCallback = mock.getArgument(4);
            pullCallback.onSuccess(pullResultExt);
            return null;
        }).when(mqClientAPI).pullMessage(anyString(), any(), anyLong(), any(CommunicationMode.class), any(PullCallback.class));

        PullResult pullResult = mqClientAPI.pullMessageAsync(BROKER_ADDR, new PullMessageRequestHeader(), TIMEOUT).get();
        assertNotNull(pullResult);
        assertEquals(2, pullResult.getMsgFoundList().size());

        Set<String> msgIdSet = pullResult.getMsgFoundList().stream().map(MessageClientIDSetter::getUniqID).collect(Collectors.toSet());
        assertTrue(msgIdSet.contains(MessageClientIDSetter.getUniqID(msg1)));
        assertTrue(msgIdSet.contains(MessageClientIDSetter.getUniqID(msg2)));
    }

    @Test
    public void testGetConsumerListByGroupAsync() throws Exception {
        List<String> clientIds = Lists.newArrayList("clientIds");
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
            response.setCode(ResponseCode.SUCCESS);
            response.makeCustomHeaderToNet();
            GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
            body.setConsumerIdList(clientIds);
            response.setBody(body.encode());
            responseFuture.putResponse(response);
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        List<String> res = mqClientAPI.getConsumerListByGroupAsync(BROKER_ADDR, new GetConsumerListByGroupRequestHeader(), TIMEOUT).get();
        assertEquals(clientIds, res);
    }

    @Test
    public void testGetEmptyConsumerListByGroupAsync() throws Exception {
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(GetConsumerListByGroupRequestHeader.class);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.makeCustomHeaderToNet();
            responseFuture.putResponse(response);
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        List<String> res = mqClientAPI.getConsumerListByGroupAsync(BROKER_ADDR, new GetConsumerListByGroupRequestHeader(), TIMEOUT).get();
        assertTrue(res.isEmpty());
    }

    @Test
    public void testGetMaxOffsetAsync() throws Exception {
        long offset = ThreadLocalRandom.current().nextLong();
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
            GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.makeCustomHeaderToNet();
            responseFuture.putResponse(response);
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(TOPIC);
        requestHeader.setQueueId(0);
        assertEquals(offset, mqClientAPI.getMaxOffset(BROKER_ADDR, requestHeader, TIMEOUT).get().longValue());
    }

    @Test
    public void testSearchOffsetAsync() throws Exception {
        long offset = ThreadLocalRandom.current().nextLong();
        doAnswer((Answer<Void>) mock -> {
            InvokeCallback invokeCallback = mock.getArgument(3);
            ResponseFuture responseFuture = new ResponseFuture(null, 0, 3000, invokeCallback, null);
            RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
            SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.makeCustomHeaderToNet();
            responseFuture.putResponse(response);
            invokeCallback.operationSuccess(responseFuture.getResponseCommand());
            return null;
        }).when(remotingClient).invokeAsync(anyString(), any(RemotingCommand.class), anyLong(), any());

        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(TOPIC);
        requestHeader.setQueueId(0);
        requestHeader.setTimestamp(System.currentTimeMillis());
        assertEquals(offset, mqClientAPI.searchOffset(BROKER_ADDR, requestHeader, TIMEOUT).get().longValue());
    }

    protected MessageExt createMessage() {
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("topic");
        messageExt.setBornHost(NetworkUtil.string2SocketAddress("127.0.0.2:8888"));
        messageExt.setStoreHost(NetworkUtil.string2SocketAddress("127.0.0.1:10911"));
        messageExt.setBody(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        MessageClientIDSetter.setUniqID(messageExt);
        return messageExt;
    }
}
