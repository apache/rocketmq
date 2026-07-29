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
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.commons.codec.DecoderException;
import org.apache.rocketmq.remoting.netty.AttributeKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

    private Channel buildChannel(SocketAddress remoteAddress, SocketAddress localAddress) {
        Channel inboundChannel = org.mockito.Mockito.mock(Channel.class);
        when(inboundChannel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)).thenReturn(false);
        when(inboundChannel.remoteAddress()).thenReturn(remoteAddress);
        when(inboundChannel.localAddress()).thenReturn(localAddress);
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
