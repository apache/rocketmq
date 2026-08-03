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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.proxy.remoting.protocol.http2proxy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class Http2ProtocolProxyHandlerTest {

    private Http2ProtocolProxyHandler http2ProtocolProxyHandler;
    @Mock
    private Channel inboundChannel;
    @Mock
    private ChannelPipeline inboundPipeline;
    @Mock
    private Channel outboundChannel;
    @Mock
    private ChannelPipeline outboundPipeline;

    @Before
    public void setUp() throws Exception {
        ConfigurationManager.initConfig();
        ConfigurationManager.getProxyConfig().setEnableRemotingLocalProxyGrpc(true);
        http2ProtocolProxyHandler = new Http2ProtocolProxyHandler();
    }

    @Test
    public void configPipeline() {
        when(inboundChannel.pipeline()).thenReturn(inboundPipeline);
        when(inboundPipeline.addLast(any(HAProxyMessageForwarder.class))).thenReturn(inboundPipeline);
        when(outboundChannel.pipeline()).thenReturn(outboundPipeline);
        when(outboundPipeline.addFirst(any(HAProxyMessageEncoder.class))).thenReturn(outboundPipeline);
        http2ProtocolProxyHandler.configPipeline(inboundChannel, outboundChannel);
    }

    @Test
    public void matchReturnsFalseForShortBuffers() {
        assertFalse(http2ProtocolProxyHandler.match(Unpooled.EMPTY_BUFFER));

        ByteBuf shortBuffer = Unpooled.wrappedBuffer(new byte[] {'P', 'R', 'I'});
        assertFalse(http2ProtocolProxyHandler.match(shortBuffer));
    }

    @Test
    public void matchReturnsTrueForHttp2PrefacePrefix() {
        ByteBuf http2Prefix = Unpooled.wrappedBuffer(new byte[] {'P', 'R', 'I', ' '});

        assertTrue(http2ProtocolProxyHandler.match(http2Prefix));
    }
}
