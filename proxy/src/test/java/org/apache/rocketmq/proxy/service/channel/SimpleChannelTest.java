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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.channel;

import java.net.InetSocketAddress;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SimpleChannelTest {

    @Test
    public void testParseValidSocketAddress() {
        SimpleChannel channel = new SimpleChannel("127.0.0.1:10911", "127.0.0.1:8080");

        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        assertEquals("127.0.0.1", remoteAddress.getHostString());
        assertEquals(10911, remoteAddress.getPort());

        InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
        assertEquals("127.0.0.1", localAddress.getHostString());
        assertEquals(8080, localAddress.getPort());
    }

    @Test
    public void testParseInvalidSocketAddressReturnsNull() {
        assertNull(new SimpleChannel(null, null).remoteAddress());
        assertNull(new SimpleChannel("", "").remoteAddress());
        assertNull(new SimpleChannel("127.0.0.1", "127.0.0.1").remoteAddress());
        assertInvalidRemoteAndLocalAddress("127.0.0.1:not-a-port");
        assertInvalidRemoteAndLocalAddress("127.0.0.1:-1");
        assertInvalidRemoteAndLocalAddress("127.0.0.1:65536");
        assertInvalidRemoteAndLocalAddress("127.0.0.1:2147483648");
        assertInvalidRemoteAndLocalAddress("127.0.0.1:");
        assertInvalidRemoteAndLocalAddress("127.0.0.1: ");
    }

    @Test
    public void testChannelManagerConsumesInvalidSocketAddress() {
        ChannelManager channelManager = new ChannelManager();
        SimpleChannel channel = channelManager.createChannel(
            ProxyContext.create()
                .setRemoteAddress("127.0.0.1:65536")
                .setLocalAddress("127.0.0.1:-1"));

        assertNull(channel.remoteAddress());
        assertNull(channel.localAddress());
    }

    private void assertInvalidRemoteAndLocalAddress(String address) {
        assertNull(new SimpleChannel(address, address).remoteAddress());
        assertNull(new SimpleChannel(address, address).localAddress());
    }
}
