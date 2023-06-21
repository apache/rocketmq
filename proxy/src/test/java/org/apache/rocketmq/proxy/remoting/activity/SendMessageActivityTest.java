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
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SendMessageActivityTest extends InitConfigTest {
    SendMessageActivity sendMessageActivity;

    @Mock
    MessagingProcessor messagingProcessorMock;
    @Mock
    MetadataService metadataServiceMock;

    String topic = "topic";
    String producerGroup = "group";
    String brokerName = "brokerName";
    @Spy
    ChannelHandlerContext ctx = new SimpleChannelHandlerContext(new SimpleChannel(null, "1", "2")) {
        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            return null;
        }
    };

    @Before
    public void setup() {
        sendMessageActivity = new SendMessageActivity(null, messagingProcessorMock);
        when(messagingProcessorMock.getMetadataService()).thenReturn(metadataServiceMock);
    }

    @Test
    public void testSendMessage() throws Exception {
        when(metadataServiceMock.getTopicMessageType(any(), eq(topic))).thenReturn(TopicMessageType.NORMAL);
        Message message = new Message(topic, "123".getBytes());
        message.putUserProperty("a", "b");
        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
        sendMessageRequestHeader.setTopic(topic);
        sendMessageRequestHeader.setProducerGroup(producerGroup);
        sendMessageRequestHeader.setDefaultTopic("");
        sendMessageRequestHeader.setDefaultTopicQueueNums(0);
        sendMessageRequestHeader.setQueueId(0);
        sendMessageRequestHeader.setSysFlag(0);
        sendMessageRequestHeader.setBname(brokerName);
        sendMessageRequestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, sendMessageRequestHeader);
        remotingCommand.setBody(message.getBody());
        remotingCommand.makeCustomHeaderToNet();

        RemotingCommand expectResponse = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "success");
        when(messagingProcessorMock.request(any(), eq(brokerName), eq(remotingCommand), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(expectResponse));
        RemotingCommand response = sendMessageActivity.processRequest0(ctx, remotingCommand, null);
        assertThat(response).isNull();
        verify(ctx, times(1)).writeAndFlush(eq(expectResponse));
    }
}