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

package org.apache.rocketmq.proxy.remoting.activity;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RecallMessageActivityTest extends InitConfigTest {
    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private static final String BROKER_NAME = "brokerName";

    private RecallMessageActivity recallMessageActivity;
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private MetadataService metadataService;

    @Spy
    private ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "1", "2")) {
        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }
    };

    @Before
    public void init() {
        recallMessageActivity = new RecallMessageActivity(null, messagingProcessor);
        when(messagingProcessor.getMetadataService()).thenReturn(metadataService);
    }

    @Test
    public void testRecallMessage_notDelayMessage() {
        when(metadataService.getTopicMessageType(any(), eq(TOPIC))).thenReturn(TopicMessageType.NORMAL);
        ProxyException exception = Assert.assertThrows(ProxyException.class, () -> {
            recallMessageActivity.processRequest0(ctx, mockRequest(), null);
        });
        Assert.assertEquals(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, exception.getCode());
    }

    @Test
    public void testRecallMessage_success() throws Exception {
        when(metadataService.getTopicMessageType(any(), eq(TOPIC))).thenReturn(TopicMessageType.DELAY);
        RemotingCommand request = mockRequest();
        RemotingCommand expectResponse = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "");
        when(messagingProcessor.request(any(), eq(BROKER_NAME), eq(request), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(expectResponse));
        RemotingCommand response = recallMessageActivity.processRequest0(ctx, request, null);
        Assert.assertNull(response);
        verify(ctx, times(1)).writeAndFlush(eq(expectResponse));
    }

    private RemotingCommand mockRequest() {
        RecallMessageRequestHeader requestHeader = new RecallMessageRequestHeader();
        requestHeader.setProducerGroup(GROUP);
        requestHeader.setTopic(TOPIC);
        requestHeader.setRecallHandle("handle");
        requestHeader.setBrokerName(BROKER_NAME);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RECALL_MESSAGE, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }
}
