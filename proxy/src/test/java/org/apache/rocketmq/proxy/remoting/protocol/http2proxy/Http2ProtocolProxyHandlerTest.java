/*
 * Copyright (c) 2007 Mockito contributors
 * This program is made available under the terms of the MIT License.
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