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
package org.apache.rocketmq.common;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.rocketmq.common.utils.NetworkUtil;
import org.junit.Assume;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NetworkUtilTest {
    @Test
    public void testGetLocalAddress() {
        String localAddress = NetworkUtil.getLocalAddress();
        assertThat(localAddress).isNotNull();
        assertThat(localAddress.length()).isGreaterThan(0);
    }

    @Test
    public void testConvert2IpStringWithIp() {
        String result = NetworkUtil.convert2IpString("127.0.0.1:9876");
        assertThat(result).isEqualTo("127.0.0.1:9876");
        assertParseFormatParse("127.0.0.1:9876");
    }

    @Test
    public void testConvert2IpStringWithHost() {
        String result = NetworkUtil.convert2IpString("localhost:9876");
        assertThat(result).isEqualTo("127.0.0.1:9876");
        assertParseFormatParse("localhost:9876");
    }

    @Test
    public void testString2SocketAddressWithBracketedIPv6Scope() {
        InetSocketAddress result = (InetSocketAddress) NetworkUtil.string2SocketAddress(
            "[fe80::1%1]:9876");

        assertThat(result.getPort()).isEqualTo(9876);
        assertThat(result.getAddress()).isNotNull();
        assertThat(result.getAddress()).isInstanceOf(Inet6Address.class);
        assertThat(((Inet6Address) result.getAddress()).getScopeId()).isEqualTo(1);

        InetSocketAddress reparsed = (InetSocketAddress) NetworkUtil.string2SocketAddress(
            NetworkUtil.socketAddress2String(result));
        assertThat(reparsed.getAddress().getHostAddress()).isEqualTo(result.getAddress().getHostAddress());
        assertThat(reparsed.getPort()).isEqualTo(result.getPort());
        assertParseFormatParse("[fe80::1%1]:9876");
    }

    @Test
    public void testString2SocketAddressWithBracketedIPv6() {
        InetSocketAddress result = (InetSocketAddress) NetworkUtil.string2SocketAddress(
            "[202:202:0:0:0:0:81:1811]:9876");

        assertThat(result.getPort()).isEqualTo(9876);
        assertThat(result.getAddress()).isNotNull();
        assertThat(result.getAddress().getHostAddress()).isEqualTo("202:202:0:0:0:0:81:1811");
        assertParseFormatParse("[202:202:0:0:0:0:81:1811]:9876");
    }

    @Test
    public void testString2SocketAddressWithRealLinkLocalIPv6Scope() throws SocketException {
        Inet6Address linkLocalAddress = getLinkLocalAddressWithInterfaceScope();
        Assume.assumeTrue("No link-local IPv6 address with interface scope is available", linkLocalAddress != null);

        String scopedHost = linkLocalAddress.getHostAddress().split("%", 2)[0]
            + "%" + linkLocalAddress.getScopedInterface().getName();
        InetSocketAddress parsed = (InetSocketAddress) NetworkUtil.string2SocketAddress(
            "[" + scopedHost + "]:9876");

        assertThat(parsed.getAddress()).isInstanceOf(Inet6Address.class);
        Inet6Address parsedAddress = (Inet6Address) parsed.getAddress();
        assertThat(parsedAddress.isLinkLocalAddress()).isTrue();
        assertThat(parsedAddress.getScopedInterface()).isNotNull();
        assertThat(parsedAddress.getScopedInterface().getName()).isEqualTo(linkLocalAddress.getScopedInterface().getName());
        assertThat(parsed.getPort()).isEqualTo(9876);

        InetSocketAddress reparsed = (InetSocketAddress) NetworkUtil.string2SocketAddress(
            NetworkUtil.socketAddress2String(parsed));
        assertThat(reparsed.getAddress().getHostAddress()).isEqualTo(parsed.getAddress().getHostAddress());
        assertThat(reparsed.getPort()).isEqualTo(parsed.getPort());
        assertParseFormatParse("[" + scopedHost + "]:9876");
    }

    @Test
    public void testString2SocketAddressWithLegacyUnbracketedIPv6() {
        InetSocketAddress result = (InetSocketAddress) NetworkUtil.string2SocketAddress(
            "202:202:0:0:0:0:81:1811:9876");

        assertThat(result.getPort()).isEqualTo(9876);
        assertThat(result.getAddress()).isNotNull();
        assertThat(result.getAddress().getHostAddress()).isEqualTo("202:202:0:0:0:0:81:1811");
        assertParseFormatParse("202:202:0:0:0:0:81:1811:9876");
    }

    @Test
    public void testConvert2IpStringWithBracketedIPv6PreservesLegacyOutputFormat() {
        String result = NetworkUtil.convert2IpString("[202:202:0:0:0:0:81:1811]:9876");

        assertThat(result).isEqualTo("202:202:0:0:0:0:81:1811:9876");
    }

    @Test
    public void testString2SocketAddressWithMalformedAddress() {
        for (String address : new String[] {
            "[202:202:0:0:0:0:81:1811:9876",
            "[202:202:0:0:0:0:81:1811]9876",
            "[202:202:0:0:0:0:81:1811]:",
            "[fe80::1%invalid-interface-name]:9876",
            "127.0.0.1",
            "127.0.0.1:",
            "127.0.0.1:not-a-port",
            "127.0.0.1:-1",
            "127.0.0.1:65536"
        }) {
            assertThatThrownBy(() -> NetworkUtil.string2SocketAddress(address))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private void assertParseFormatParse(String address) {
        InetSocketAddress parsed = (InetSocketAddress) NetworkUtil.string2SocketAddress(address);
        String formatted = NetworkUtil.socketAddress2String(parsed);
        InetSocketAddress reparsed = (InetSocketAddress) NetworkUtil.string2SocketAddress(formatted);

        assertThat(reparsed.getAddress().getHostAddress()).isEqualTo(parsed.getAddress().getHostAddress());
        assertThat(reparsed.getPort()).isEqualTo(parsed.getPort());
    }

    private Inet6Address getLinkLocalAddressWithInterfaceScope() throws SocketException {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            if (!networkInterface.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet6Address) {
                    Inet6Address inet6Address = (Inet6Address) address;
                    if (inet6Address.isLinkLocalAddress() && inet6Address.getScopedInterface() != null) {
                        return inet6Address;
                    }
                }
            }
        }
        return null;
    }
}
