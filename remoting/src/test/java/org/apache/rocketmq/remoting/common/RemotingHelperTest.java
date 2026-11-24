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
package org.apache.rocketmq.remoting.common;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemotingHelperTest {
    private static final int SMALL_BUF = 16 * 1024;
    private static final int LARGE_BUF = 256 * 1024;

    /**
     * connect() must drive the socket send buffer from socketSndbufSize and the receive buffer from
     * socketRcvbufSize. The two were wired to the opposite socket options, so a caller that tuned these knobs got
     * the send and receive buffer sizes swapped.
     *
     * The OS clamps and scales requested buffer sizes, so the exact value cannot be asserted. Instead we open two
     * connections that differ only in socketSndbufSize and read back the same socket option (the send buffer) from
     * both. The reported send buffer tracks the requested send buffer monotonically, so the connection configured
     * with the smaller socketSndbufSize must end up with the smaller send buffer. With the knobs swapped the send
     * buffer follows socketRcvbufSize instead, which inverts the comparison.
     */
    @Test
    public void testConnectAppliesSndAndRcvBufferToMatchingSocketOption() throws Exception {
        int originalSnd = NettySystemConfig.socketSndbufSize;
        int originalRcv = NettySystemConfig.socketRcvbufSize;

        ServerSocket serverSocket = new ServerSocket(0);
        SocketAddress remote = new InetSocketAddress("127.0.0.1", serverSocket.getLocalPort());
        SocketChannel smallSnd = null;
        SocketChannel largeSnd = null;
        try {
            NettySystemConfig.socketSndbufSize = SMALL_BUF;
            NettySystemConfig.socketRcvbufSize = LARGE_BUF;
            smallSnd = RemotingHelper.connect(remote);

            NettySystemConfig.socketSndbufSize = LARGE_BUF;
            NettySystemConfig.socketRcvbufSize = SMALL_BUF;
            largeSnd = RemotingHelper.connect(remote);

            assertThat(smallSnd).isNotNull();
            assertThat(largeSnd).isNotNull();
            assertThat(smallSnd.socket().getSendBufferSize())
                .isLessThan(largeSnd.socket().getSendBufferSize());
        } finally {
            if (smallSnd != null) {
                smallSnd.close();
            }
            if (largeSnd != null) {
                largeSnd.close();
            }
            serverSocket.close();
            NettySystemConfig.socketSndbufSize = originalSnd;
            NettySystemConfig.socketRcvbufSize = originalRcv;
        }
    }
    @Test
    public void testIpInCIDRWithZeroPrefixMatchesEveryAddress() {
        // A /0 prefix is the catch-all CIDR and must match every IPv4 address,
        // mirroring IPAddressUtils.isIPInRange(ip, "0.0.0.0/0").
        assertThat(RemotingHelper.ipInCIDR("0.0.0.0", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("10.0.0.1", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.1.1", "0.0.0.0/0")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("255.255.255.255", "0.0.0.0/0")).isTrue();
    }

    @Test
    public void testIpInCIDRWithFullPrefixRequiresExactAddress() {
        assertThat(RemotingHelper.ipInCIDR("192.168.1.1", "192.168.1.1/32")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.1.2", "192.168.1.1/32")).isFalse();
    }

    @Test
    public void testIpInCIDRWithNormalPrefix() {
        assertThat(RemotingHelper.ipInCIDR("192.168.1.100", "192.168.1.0/24")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.2.1", "192.168.1.0/24")).isFalse();
        assertThat(RemotingHelper.ipInCIDR("10.1.2.3", "10.0.0.0/8")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("11.1.2.3", "10.0.0.0/8")).isFalse();
    }

    @Test
    public void testIpInCIDRMasksHostBitsOfCidr() {
        // Host bits of the CIDR must be ignored: 192.168.1.5/24 behaves like 192.168.1.0/24.
        assertThat(RemotingHelper.ipInCIDR("192.168.1.200", "192.168.1.5/24")).isTrue();
        assertThat(RemotingHelper.ipInCIDR("192.168.2.200", "192.168.1.5/24")).isFalse();
    }
}
