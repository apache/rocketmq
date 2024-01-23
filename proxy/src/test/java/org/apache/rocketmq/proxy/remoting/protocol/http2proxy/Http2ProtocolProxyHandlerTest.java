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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
        http2ProtocolProxyHandler = new Http2ProtocolProxyHandler();
    }

    @Test
    public void configPipeline() {
        when(inboundChannel.hasAttr(eq(AttributeKeys.PROXY_PROTOCOL_ADDR))).thenReturn(true);
        when(inboundChannel.pipeline()).thenReturn(inboundPipeline);
        when(inboundPipeline.addLast(any(HAProxyMessageForwarder.class))).thenReturn(inboundPipeline);
        when(outboundChannel.pipeline()).thenReturn(outboundPipeline);
        when(outboundPipeline.addFirst(any(HAProxyMessageEncoder.class))).thenReturn(outboundPipeline);
        http2ProtocolProxyHandler.configPipeline(inboundChannel, outboundChannel);
    }
}