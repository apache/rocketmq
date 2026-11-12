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
}
