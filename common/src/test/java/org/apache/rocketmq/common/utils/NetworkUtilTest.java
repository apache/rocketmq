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

package org.apache.rocketmq.common.utils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NetworkUtilTest {

    @Test
    public void testString2SocketAddressIPv4() {
        SocketAddress addr = NetworkUtil.string2SocketAddress("127.0.0.1:9876");
        InetSocketAddress inetAddr = (InetSocketAddress) addr;
        assertThat(inetAddr.getHostString()).isEqualTo("127.0.0.1");
        assertThat(inetAddr.getPort()).isEqualTo(9876);
        assertThat(inetAddr.getAddress()).isNotNull();
    }

    @Test
    public void testString2SocketAddressIPv6WithBrackets() {
        String ipv6 = "[240e:341:6246:c700:c4ad:e645:459e:2]:44880";
        SocketAddress addr = NetworkUtil.string2SocketAddress(ipv6);
        InetSocketAddress inetAddr = (InetSocketAddress) addr;
        assertThat(inetAddr.getPort()).isEqualTo(44880);
        assertThat(inetAddr.getAddress()).isNotNull();
    }

    @Test
    public void testString2SocketAddressIPv6Loopback() {
        String ipv6 = "[::1]:8080";
        SocketAddress addr = NetworkUtil.string2SocketAddress(ipv6);
        InetSocketAddress inetAddr = (InetSocketAddress) addr;
        assertThat(inetAddr.getPort()).isEqualTo(8080);
        assertThat(inetAddr.getAddress()).isNotNull();
    }

    @Test
    public void testSocketAddress2StringIPv4() {
        String addr = "127.0.0.1:9876";
        SocketAddress sa = NetworkUtil.string2SocketAddress(addr);
        String result = NetworkUtil.socketAddress2String(sa);
        assertThat(result).isEqualTo(addr);
    }

    @Test
    public void testNormalizeAndDenormalizeHostAddress() {
        String normalized = "[::1]";
        String denormalized = NetworkUtil.denormalizeHostAddress(normalized);
        assertThat(denormalized).isEqualTo("::1");

        String noBracket = "127.0.0.1";
        assertThat(NetworkUtil.denormalizeHostAddress(noBracket)).isEqualTo("127.0.0.1");

        assertThat(NetworkUtil.denormalizeHostAddress(null)).isNull();
    }
}
