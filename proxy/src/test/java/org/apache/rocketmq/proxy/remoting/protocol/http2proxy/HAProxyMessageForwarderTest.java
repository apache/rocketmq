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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.codec.DecoderException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HAProxyMessageForwarderTest {

    private HAProxyMessageForwarder haProxyMessageForwarder;

    @Mock
    private Channel outboundChannel;

    @Before
    public void setUp() throws Exception {
        haProxyMessageForwarder = new HAProxyMessageForwarder(outboundChannel);
    }

    @Test
    public void buildHAProxyTLV() throws DecoderException {
        HAProxyTLV haProxyTLV = haProxyMessageForwarder.buildHAProxyTLV("proxy_protocol_tlv_0xe1", "xxxx");
        assertNotNull(haProxyTLV);
        assertEquals((byte) 0xe1, haProxyTLV.typeByteValue());
    }

    @Test
    public void buildHAProxyMessageWithValidChannelAddress() throws Exception {
        Channel inboundChannel = buildChannel(
            new InetSocketAddress("127.0.0.1", 10911),
            new InetSocketAddress("127.0.0.2", 8081)
        );

        HAProxyMessage haProxyMessage = haProxyMessageForwarder.buildHAProxyMessage(inboundChannel);

        assertNotNull(haProxyMessage);
        assertEquals("127.0.0.1", haProxyMessage.sourceAddress());
        assertEquals(10911, haProxyMessage.sourcePort());
        assertEquals("127.0.0.2", haProxyMessage.destinationAddress());
        assertEquals(8081, haProxyMessage.destinationPort());
    }

    @Test
    public void buildHAProxyMessageReturnsNullWhenChannelPortIsInvalid() throws Exception {
        Channel inboundChannel = buildChannel(
            stringSocketAddress("127.0.0.1:not-a-port"),
            new InetSocketAddress("127.0.0.2", 8081)
        );

        assertNull(haProxyMessageForwarder.buildHAProxyMessage(inboundChannel));
    }

    @Test
    public void buildHAProxyMessageReturnsNullWhenLocalChannelPortIsInvalid() throws Exception {
        Channel inboundChannel = buildChannel(
            new InetSocketAddress("127.0.0.1", 10911),
            stringSocketAddress("127.0.0.2:not-a-port")
        );

        assertNull(haProxyMessageForwarder.buildHAProxyMessage(inboundChannel));
    }

    @Test
    public void buildHAProxyMessageWithProxyProtocolAttributes() throws Exception {
        Channel inboundChannel = buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", "8081");

        HAProxyMessage haProxyMessage = haProxyMessageForwarder.buildHAProxyMessage(inboundChannel);

        assertNotNull(haProxyMessage);
        assertEquals("127.0.0.1", haProxyMessage.sourceAddress());
        assertEquals(10911, haProxyMessage.sourcePort());
        assertEquals("127.0.0.2", haProxyMessage.destinationAddress());
        assertEquals(8081, haProxyMessage.destinationPort());
    }

    @Test
    public void buildHAProxyMessageThrowsWhenProxyProtocolSourcePortIsInvalid() throws Exception {
        Channel inboundChannel = buildProxyProtocolChannel("127.0.0.1", "not-a-port", "127.0.0.2", "8081");

        assertInvalidHAProxyMetadata(inboundChannel);
    }

    @Test
    public void buildHAProxyMessageThrowsWhenProxyProtocolDestinationPortIsInvalid() throws Exception {
        Channel inboundChannel = buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", "not-a-port");

        assertInvalidHAProxyMetadata(inboundChannel);
    }

    @Test
    public void buildHAProxyMessageThrowsWhenProxyProtocolAttributeIsMissingOrEmpty() throws Exception {
        assertInvalidHAProxyMetadata(buildProxyProtocolChannel(null, "10911", "127.0.0.2", "8081"));
        assertInvalidHAProxyMetadata(buildProxyProtocolChannel("127.0.0.1", "", "127.0.0.2", "8081"));
        assertInvalidHAProxyMetadata(buildProxyProtocolChannel("127.0.0.1", "10911", "", "8081"));
        assertInvalidHAProxyMetadata(buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", null));
        assertInvalidHAProxyMetadata(buildProxyProtocolChannelWithoutSourceAddress());
        assertInvalidHAProxyMetadata(buildProxyProtocolChannelWithoutSourcePort());
        assertInvalidHAProxyMetadata(buildProxyProtocolChannelWithoutDestinationAddress());
        assertInvalidHAProxyMetadata(buildProxyProtocolChannelWithoutDestinationPort());
    }

    @Test
    public void buildHAProxyMessageWithProxyProtocolIpv6Attributes() throws Exception {
        Channel inboundChannel = buildProxyProtocolChannel("2001:db8::1", "0", "2001:db8::2", "65535");

        HAProxyMessage haProxyMessage = haProxyMessageForwarder.buildHAProxyMessage(inboundChannel);

        assertNotNull(haProxyMessage);
        assertEquals("2001:db8::1", haProxyMessage.sourceAddress());
        assertEquals(0, haProxyMessage.sourcePort());
        assertEquals("2001:db8::2", haProxyMessage.destinationAddress());
        assertEquals(65535, haProxyMessage.destinationPort());
    }

    @Test
    public void channelReadRejectsMalformedProxyProtocolSourcePort() {
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "not-a-port", "127.0.0.2", "8081"));
    }

    @Test
    public void channelReadRejectsMalformedProxyProtocolDestinationPort() {
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", "not-a-port"));
    }

    @Test
    public void channelReadRejectsOutOfRangeProxyProtocolPorts() {
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "-1", "127.0.0.2", "8081"));
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "65536", "127.0.0.2", "8081"));
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", "-1"));
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", "65536"));
    }

    @Test
    public void channelReadRejectsMissingProxyProtocolAttributes() {
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannelWithoutSourceAddress());
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannelWithoutSourcePort());
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannelWithoutDestinationAddress());
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannelWithoutDestinationPort());
    }

    @Test
    public void channelReadRejectsEmptyProxyProtocolAttributes() {
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel(null, "10911", "127.0.0.2", "8081"));
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "", "127.0.0.2", "8081"));
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "10911", "", "8081"));
        assertRejectsMalformedProxyProtocol(buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", null));
    }

    @Test
    public void channelReadClosesWhenWriteAndFlushFails() throws Exception {
        assertWriteFailureClosesChannel(new IllegalStateException("write failed"));
    }

    @Test
    public void channelReadClosesWhenWriteAndFlushIsCancelled() throws Exception {
        assertWriteFailureClosesChannel(new CancellationException("write cancelled"));
    }

    @Test
    public void channelReadClosesWhenWriteAndFlushIsInterrupted() throws Exception {
        assertWriteFailureClosesChannel(new InterruptedException("write interrupted"));
    }

    @Test
    public void parsePortReturnsNullWhenPortIsOutOfRange() {
        assertNull(haProxyMessageForwarder.parsePort("-1"));
        assertNull(haProxyMessageForwarder.parsePort("65536"));
        assertEquals(Integer.valueOf(0), haProxyMessageForwarder.parsePort("0"));
        assertEquals(Integer.valueOf(65535), haProxyMessageForwarder.parsePort("65535"));
    }

    private void assertInvalidHAProxyMetadata(Channel inboundChannel) throws Exception {
        try {
            haProxyMessageForwarder.buildHAProxyMessage(inboundChannel);
            fail();
        } catch (HAProxyMessageForwarder.InvalidHAProxyMetadataException ignored) {
        }
    }

    private void assertRejectsMalformedProxyProtocol(EmbeddedChannel inboundChannel) {
        AtomicBoolean fired = new AtomicBoolean(false);
        EmbeddedChannel outboundChannel = new EmbeddedChannel();
        inboundChannel.pipeline().addLast(new HAProxyMessageForwarder(outboundChannel), new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                fired.set(true);
                ReferenceCountUtil.release(msg);
            }
        });

        inboundChannel.writeInbound(new Object());

        assertFalse(fired.get());
        assertFalse(inboundChannel.isOpen());
        assertNull(outboundChannel.readOutbound());
    }

    private void assertWriteFailureClosesChannel(Throwable failure) throws Exception {
        ChannelFuture channelFuture = org.mockito.Mockito.mock(ChannelFuture.class);
        when(outboundChannel.writeAndFlush(any())).thenReturn(channelFuture);
        if (failure instanceof InterruptedException) {
            when(channelFuture.sync()).thenThrow((InterruptedException) failure);
        } else if (failure instanceof RuntimeException) {
            when(channelFuture.sync()).thenThrow((RuntimeException) failure);
        } else {
            throw new IllegalArgumentException("unsupported failure", failure);
        }

        AtomicBoolean fired = new AtomicBoolean(false);
        EmbeddedChannel inboundChannel = buildProxyProtocolChannel("127.0.0.1", "10911", "127.0.0.2", "8081");
        inboundChannel.pipeline().addLast(haProxyMessageForwarder, new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                fired.set(true);
                ReferenceCountUtil.release(msg);
            }
        });

        try {
            inboundChannel.writeInbound(new Object());
            fail();
        } catch (Exception ignored) {
        }

        assertFalse(fired.get());
        assertFalse(inboundChannel.isOpen());
        verify(outboundChannel).writeAndFlush(any(HAProxyMessage.class));
    }

    private Channel buildChannel(SocketAddress remoteAddress, SocketAddress localAddress) {
        Channel inboundChannel = org.mockito.Mockito.mock(Channel.class);
        when(inboundChannel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)).thenReturn(false);
        when(inboundChannel.remoteAddress()).thenReturn(remoteAddress);
        when(inboundChannel.localAddress()).thenReturn(localAddress);
        return inboundChannel;
    }

    private EmbeddedChannel buildProxyProtocolChannel(String sourceAddress, String sourcePort,
        String destinationAddress, String destinationPort) {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_ADDR).set(sourceAddress);
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_PORT).set(sourcePort);
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR).set(destinationAddress);
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_PORT).set(destinationPort);
        return inboundChannel;
    }

    private EmbeddedChannel buildProxyProtocolChannelWithoutSourcePort() {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_ADDR).set("127.0.0.1");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR).set("127.0.0.2");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_PORT).set("8081");
        return inboundChannel;
    }

    private EmbeddedChannel buildProxyProtocolChannelWithoutSourceAddress() {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_PORT).set("10911");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR).set("127.0.0.2");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_PORT).set("8081");
        return inboundChannel;
    }

    private EmbeddedChannel buildProxyProtocolChannelWithoutDestinationPort() {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_ADDR).set("127.0.0.1");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_PORT).set("10911");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_ADDR).set("127.0.0.2");
        return inboundChannel;
    }

    private EmbeddedChannel buildProxyProtocolChannelWithoutDestinationAddress() {
        EmbeddedChannel inboundChannel = new EmbeddedChannel();
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_ADDR).set("127.0.0.1");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_PORT).set("10911");
        inboundChannel.attr(AttributeKeys.PROXY_PROTOCOL_SERVER_PORT).set("8081");
        return inboundChannel;
    }

    private SocketAddress stringSocketAddress(String value) {
        return new SocketAddress() {
            @Override
            public String toString() {
                return value;
            }
        };
    }
}
