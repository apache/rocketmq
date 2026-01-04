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
package org.apache.rocketmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.haproxy.HAProxyTLV;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NettyRemotingServerTest {

    private NettyRemotingServer nettyRemotingServer;

    @Mock
    private Channel channel;

    @Mock
    private Attribute attribute;

    @Before
    public void setUp() throws Exception {
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyRemotingServer = new NettyRemotingServer(nettyServerConfig);
    }

    @Test
    public void handleHAProxyTLV() {
        when(channel.attr(any(AttributeKey.class))).thenReturn(attribute);
        doNothing().when(attribute).set(any());

        ByteBuf content = Unpooled.buffer();
        content.writeBytes("xxxx".getBytes(StandardCharsets.UTF_8));
        HAProxyTLV haProxyTLV = new HAProxyTLV((byte) 0xE1, content);
        nettyRemotingServer.handleHAProxyTLV(haProxyTLV, channel);
    }
}