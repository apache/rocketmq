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

package org.apache.rocketmq.remoting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class ProxyProtocolTest {

    private RemotingServer remotingServer;
    private RemotingClient remotingClient;

    @Before
    public void setUp() throws Exception {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setUseTLS(false);

        remotingServer = RemotingServerTest.createRemotingServer();
        remotingClient = RemotingServerTest.createRemotingClient(clientConfig);

        await().pollDelay(Duration.ofMillis(10))
                .pollInterval(Duration.ofMillis(10))
                .atMost(20, TimeUnit.SECONDS).until(() -> isHostConnectable(getServerAddress()));
    }

    @Test
    public void testProxyProtocol() throws Exception {
        sendHAProxyMessage(remotingClient);
        requestThenAssertResponse(remotingClient);
    }

    private void requestThenAssertResponse(RemotingClient remotingClient) throws Exception {
        RemotingCommand response = remotingClient.invokeSync(getServerAddress(), createRequest(), 10000 * 3);
        assertNotNull(response);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);
        assertThat(response.getExtFields().get("messageTitle")).isEqualTo("Welcome");
    }

    private void sendHAProxyMessage(RemotingClient remotingClient) throws Exception {
        Method getAndCreateChannel = NettyRemotingClient.class.getDeclaredMethod("getAndCreateChannel", String.class);
        getAndCreateChannel.setAccessible(true);
        NettyRemotingClient nettyRemotingClient = (NettyRemotingClient) remotingClient;
        Channel channel = (Channel) getAndCreateChannel.invoke(nettyRemotingClient, getServerAddress());
        HAProxyMessage message = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4, "127.0.0.1", "127.0.0.2", 8000, 9000);

        ByteBuf byteBuf = Unpooled.directBuffer();
        Method encode = HAProxyMessageEncoder.class.getDeclaredMethod("encodeV2", HAProxyMessage.class, ByteBuf.class);
        encode.setAccessible(true);
        encode.invoke(HAProxyMessageEncoder.INSTANCE, message, byteBuf);
        channel.writeAndFlush(byteBuf).sync();
    }

    private static RemotingCommand createRequest() {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        return RemotingCommand.createRequestCommand(0, requestHeader);
    }


    private String getServerAddress() {
        return "localhost:" + remotingServer.localListenPort();
    }

    private boolean isHostConnectable(String addr) {
        try (Socket socket = new Socket()) {
            socket.connect(NetworkUtil.string2SocketAddress(addr));
            return true;
        } catch (IOException ignored) {
        }
        return false;
    }
}
