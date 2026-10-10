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

package org.apache.rocketmq.proxy.service.channel;

import java.net.InetSocketAddress;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleChannelTest {

    @Test
    public void testRemoteAddressIPv4() {
        SimpleChannel channel = new SimpleChannel("127.0.0.1:8080", "127.0.0.1:8081");
        InetSocketAddress remote = (InetSocketAddress) channel.remoteAddress();
        assertThat(remote.getHostString()).isEqualTo("127.0.0.1");
        assertThat(remote.getPort()).isEqualTo(8080);
        assertThat(remote.getAddress()).isNotNull();
    }

    @Test
    public void testRemoteAddressIPv6WithBrackets() {
        SimpleChannel channel = new SimpleChannel("[240e:341:6246:c700:c4ad:e645:459e:2]:44880", "127.0.0.1:8081");
        InetSocketAddress remote = (InetSocketAddress) channel.remoteAddress();
        assertThat(remote.getPort()).isEqualTo(44880);
        assertThat(remote.getAddress()).isNotNull();
    }

    @Test
    public void testLocalAddressIPv6Loopback() {
        SimpleChannel channel = new SimpleChannel("127.0.0.1:8080", "[::1]:8081");
        InetSocketAddress local = (InetSocketAddress) channel.localAddress();
        assertThat(local.getPort()).isEqualTo(8081);
        assertThat(local.getAddress()).isNotNull();
    }

    @Test
    public void testRemoteAddressNull() {
        SimpleChannel channel = new SimpleChannel("", "127.0.0.1:8081");
        assertThat(channel.remoteAddress()).isNull();
    }

    @Test
    public void testRemoteAddressNoPort() {
        SimpleChannel channel = new SimpleChannel("127.0.0.1", "127.0.0.1:8081");
        assertThat(channel.remoteAddress()).isNull();
    }
}
