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

import java.net.InetAddress;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NetworkUtilTest {
    @Test
    public void testGetLocalAddress() {
        String localAddress = NetworkUtil.getLocalAddress();
        assertThat(localAddress).isNotNull();
        assertThat(localAddress.length()).isGreaterThan(0);
        assertThat(localAddress).isNotEqualTo(InetAddress.getLoopbackAddress().getHostAddress());
    }

    @Test
    public void testConvert2IpStringWithIp() {
        String result = NetworkUtil.convert2IpString("127.0.0.1:9876");
        assertThat(result).isEqualTo("127.0.0.1:9876");
    }

    @Test
    public void testConvert2IpStringWithHost() {
        String result = NetworkUtil.convert2IpString("localhost:9876");
        assertThat(result).isEqualTo("127.0.0.1:9876");
    }

    @Test
    public void testGetHostLength() {
        // IPv4 host = ip(4 byte) + port(4 byte)
        // IPv6 host = ip(16 byte) + port(4 byte)

        int ipV4WithoutPort = NetworkUtil.getHostLength(false, false);
        assertThat(ipV4WithoutPort).isEqualTo(4);

        int ipV4WithPort = NetworkUtil.getHostLength(false, true);
        assertThat(ipV4WithPort).isEqualTo(8);

        int ipV6WithoutPort = NetworkUtil.getHostLength(true, false);
        assertThat(ipV6WithoutPort).isEqualTo(16);

        int ipV6WithPort = NetworkUtil.getHostLength(true, true);
        assertThat(ipV6WithPort).isEqualTo(20);
    }
}
