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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReplyMessageProcessorTest {
    private ReplyMessageProcessor replyMessageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());
    @Mock
    private ChannelHandlerContext handlerContext;
    @Mock
    private MessageStore messageStore;
    @Mock
    private Channel channel;

    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ClientChannelInfo clientInfo;
    @Mock
    private Broker2Client broker2Client;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        clientInfo = new ClientChannelInfo(channel, "127.0.0.1", LanguageCode.JAVA, 0);
        brokerController.setMessageStore(messageStore);
        Field field = BrokerController.class.getDeclaredField("broker2Client");
        field.setAccessible(true);
        field.set(brokerController, broker2Client);
        replyMessageProcessor = new ReplyMessageProcessor(brokerController);
    }

    @Test
    public void testProcessRequest_Success() throws RemotingCommandException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException {
        brokerController.getProducerManager().registerProducer(group, clientInfo);
        final RemotingCommand request = createReplyMessageRequestHeaderCommand(RequestCode.SEND_REPLY_MESSAGE, clientInfo.getClientId());
        when(brokerController.getBroker2Client().callClient(any(), any(RemotingCommand.class))).thenReturn(createResponse(ResponseCode.SUCCESS, request));

        boolean res = replyMessageProcessor.pushReplyMessageToProducer(request, (ReplyMessageRequestHeader) request.decodeCommandCustomHeader(ReplyMessageRequestHeader.class));
        assertThat(res).isEqualTo(Boolean.TRUE);
    }

    private RemotingCommand createReplyMessageRequestHeaderCommand(int requestCode, String clientId) {
        ReplyMessageRequestHeader requestHeader = createSendMessageRequestHeader(clientId);
        RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, requestHeader);
        request.setBody(new byte[] {'a'});
        request.makeCustomHeaderToNet();
        return request;
    }

    private ReplyMessageRequestHeader createSendMessageRequestHeader(String requestClientId) {
        ReplyMessageRequestHeader requestHeader = new ReplyMessageRequestHeader();
        requestHeader.setConsumerGroup("consumerGroup");
        requestHeader.setConsumerResult("CONSUME_SUCCESS");
        requestHeader.setConsumerTimeStamp(System.currentTimeMillis());
        requestHeader.setTopic("TestTopic");
        requestHeader.setFlag(0);
        requestHeader.setTransactionId(null);

        Map<String, String> map = new HashMap<>();
        map.put(MessageConst.PROPERTY_CORRELATION_ID, CorrelationIdUtil.createCorrelationId());
        map.put(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        map.put(MessageConst.PROPERTY_MESSAGE_REPLY_TTL, "30000");
        map.put(MessageConst.PROPERTY_MESSAGE_REPLY_SEND_TIME, String.valueOf(System.currentTimeMillis()));
        requestHeader.setProperties(MessageDecoder.messageProperties2String(map));
        return requestHeader;
    }

    private RemotingCommand createResponse(int code, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        response.setCode(code);
        response.setOpaque(request.getOpaque());
        return response;
    }
}
