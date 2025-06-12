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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryMessageProcessorTest {
    private QueryMessageProcessor queryMessageProcessor;
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    @Mock
    private MessageStore messageStore;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private Channel channel;

    @Mock
    private ChannelFuture channelFuture;

    @Before
    public void init() {
        when(handlerContext.channel()).thenReturn(channel);
        queryMessageProcessor = new QueryMessageProcessor(brokerController);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(channel.writeAndFlush(any())).thenReturn(channelFuture);
    }

    @Test
    public void testQueryMessage() throws RemotingCommandException {
        QueryMessageResult result = new QueryMessageResult();
        result.setIndexLastUpdateTimestamp(100);
        result.setIndexLastUpdatePhyoffset(0);
        result.addMessage(new SelectMappedBufferResult(0, null, 0, null));

        when(messageStore.queryMessage(anyString(),anyString(),anyInt(),anyLong(),anyLong())).thenReturn(result);
        RemotingCommand request = createQueryMessageRequest("topic", "msgKey", 1, 100, 200,"false");
        request.makeCustomHeaderToNet();
        RemotingCommand response = queryMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(response.getCode(), ResponseCode.QUERY_NOT_FOUND);

        result.addMessage(new SelectMappedBufferResult(0, null, 1, null));
        when(messageStore.queryMessage(anyString(),anyString(),anyInt(),anyLong(),anyLong())).thenReturn(result);
        response = queryMessageProcessor.processRequest(handlerContext, request);
        Assert.assertNull(response);
    }

    @Test
    public void testViewMessageById() throws RemotingCommandException {
        ViewMessageRequestHeader viewMessageRequestHeader = new ViewMessageRequestHeader();
        viewMessageRequestHeader.setTopic("topic");
        viewMessageRequestHeader.setOffset(0L);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, viewMessageRequestHeader);
        request.makeCustomHeaderToNet();
        request.setCode(RequestCode.VIEW_MESSAGE_BY_ID);

        when(messageStore.selectOneMessageByOffset(anyLong())).thenReturn(null);
        RemotingCommand response = queryMessageProcessor.processRequest(handlerContext, request);
        Assert.assertEquals(response.getCode(), ResponseCode.SYSTEM_ERROR);

        when(messageStore.selectOneMessageByOffset(anyLong())).thenReturn(new SelectMappedBufferResult(0, null, 0, null));
        response = queryMessageProcessor.processRequest(handlerContext, request);
        Assert.assertNull(response);
    }

    private RemotingCommand createQueryMessageRequest(String topic, String key, int maxNum, long beginTimestamp, long endTimestamp,String flag) {
        QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setKey(key);
        requestHeader.setMaxNum(maxNum);
        requestHeader.setBeginTimestamp(beginTimestamp);
        requestHeader.setEndTimestamp(endTimestamp);

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(MixAll.UNIQUE_MSG_QUERY_FLAG, flag);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.setExtFields(extFields);
        return request;
    }
}
