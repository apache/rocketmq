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
package org.apache.rocketmq.ratelimit.provider;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.rocketmq.ratelimit.context.RatelimitContext;
import org.apache.rocketmq.ratelimit.helper.RatelimitTestHelper;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RatelimitProviderTest {
    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Mock
    private Channel channel;

    RatelimitProvider provider = new DefaultRatelimitProvider();

    @Before
    public void setUp() throws Exception {
        provider.initialize(RatelimitTestHelper.createDefaultConfig());
    }

    @Test
    public void newContext() {
        when(channel.id()).thenReturn(mockChannelId("channel-id"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(mockAttribute("192.168.0.1"));
        when(channel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(true);
        when(channel.attr(eq(AttributeKeys.PROXY_PROTOCOL_PORT))).thenReturn(mockAttribute("1234"));
        when(channelHandlerContext.channel()).thenReturn(channel);

        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
        sendMessageRequestHeader.setTopic("topic");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, sendMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RatelimitContext context = provider.newContext(channelHandlerContext, request);
        Assert.assertEquals("topic", context.getTopic());
        Assert.assertEquals("rocketmq", context.getAccessKey());
        Assert.assertEquals("192.168.0.1", context.getSourceIp());
        Assert.assertEquals("channel-id", context.getChannelId());

        SendMessageRequestHeaderV2 sendMessageRequestHeaderV2 = new SendMessageRequestHeaderV2();
        sendMessageRequestHeaderV2.setTopic("topic");
        request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, sendMessageRequestHeaderV2);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        context = provider.newContext(channelHandlerContext, request);
        Assert.assertEquals("topic", context.getTopic());
        Assert.assertEquals("rocketmq", context.getAccessKey());
        Assert.assertEquals("192.168.0.1", context.getSourceIp());
        Assert.assertEquals("channel-id", context.getChannelId());

        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topic");
        pullMessageRequestHeader.setConsumerGroup("group");
        request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        context = provider.newContext(channelHandlerContext, request);
        Assert.assertEquals("topic", context.getTopic());
        Assert.assertEquals("rocketmq", context.getAccessKey());
        Assert.assertEquals("192.168.0.1", context.getSourceIp());
        Assert.assertEquals("channel-id", context.getChannelId());
    }

    private ChannelId mockChannelId(String channelId) {
        return new ChannelId() {
            @Override
            public String asShortText() {
                return channelId;
            }

            @Override
            public String asLongText() {
                return channelId;
            }

            @Override
            public int compareTo(ChannelId o) {
                return 0;
            }
        };
    }

    private Attribute<String> mockAttribute(String value) {
        return new Attribute<String>() {
            @Override
            public AttributeKey<String> key() {
                return null;
            }

            @Override
            public String get() {
                return value;
            }

            @Override
            public void set(String value) {
            }

            @Override
            public String getAndSet(String value) {
                return null;
            }

            @Override
            public String setIfAbsent(String value) {
                return null;
            }

            @Override
            public String getAndRemove() {
                return null;
            }

            @Override
            public boolean compareAndSet(String oldValue, String newValue) {
                return false;
            }

            @Override
            public void remove() {

            }
        };
    }
}