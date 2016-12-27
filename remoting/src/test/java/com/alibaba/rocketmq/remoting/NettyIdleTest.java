/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.*;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;


/**
 * @author shijia.wxr
 *
 */
public class NettyIdleTest {
    // @Test
    public void test_idle_event() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RemotingServer server = createRemotingServer();
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 10; i++) {
            RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
            RemotingCommand response = client.invokeSync("localhost:8888", request, 1000 * 3);
            System.out.println(i + " invoke result = " + response);
            assertTrue(response != null);

            Thread.sleep(1000 * 10);
        }

        Thread.sleep(1000 * 60);

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }

    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        config.setServerChannelMaxIdleTimeSeconds(30);
        RemotingServer remotingServer = new NettyRemotingServer(config);
        remotingServer.registerProcessor(0, new NettyRequestProcessor() {
            private int i = 0;


            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                System.out.println("processRequest=" + request + " " + (i++));
                request.setRemark("hello, I am respponse " + ctx.channel().remoteAddress());
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newCachedThreadPool());
        remotingServer.start();
        return remotingServer;
    }

    public static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(15);
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }

}
