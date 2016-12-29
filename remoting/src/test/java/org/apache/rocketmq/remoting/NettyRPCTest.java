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

/**
 * $Id: NettyRPCTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.Executors;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class NettyRPCTest {
    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
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
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }

    @BeforeClass
    public static void initialize() throws InterruptedException {
        remotingServer = createRemotingServer();
        remotingClient = createRemotingClient();
    }

    @AfterClass
    public static void destroy() {
        remotingClient.shutdown();
        remotingServer.shutdown();
    }

    @Test
    public void test_RPC_Sync() throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException {

        for (int i = 0; i < 100; i++) {
            TestRequestHeader requestHeader = new TestRequestHeader();
            requestHeader.setCount(i);
            requestHeader.setMessageTitle("HelloMessageTitle");
            RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 3000);
            System.out.println("invoke result = " + response);
            assertTrue(response != null);
        }
    }

    @Test
    public void test_RPC_Oneway() throws InterruptedException, RemotingConnectException,
        RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        for (int i = 0; i < 100; i++) {
            RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
            request.setRemark(String.valueOf(i));
            remotingClient.invokeOneway("localhost:8888", request, 1000 * 3);
        }
    }

    @Test
    public void test_RPC_Async() throws InterruptedException, RemotingConnectException,
        RemotingTimeoutException, RemotingTooMuchRequestException, RemotingSendRequestException {

        for (int i = 0; i < 100; i++) {
            RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
            request.setRemark(String.valueOf(i));
            remotingClient.invokeAsync("localhost:8888", request, 1000 * 3, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    System.out.println(responseFuture.getResponseCommand());
                }
            });
        }
    }

    @Test
    public void test_server_call_client() throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException {

        remotingServer.registerProcessor(0, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                try {
                    return remotingServer.invokeSync(ctx.channel(), request, 1000 * 10);
                } catch (InterruptedException | RemotingSendRequestException | RemotingTimeoutException e) {
                    e.printStackTrace();
                }

                return null;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newCachedThreadPool());

        remotingClient.registerProcessor(0, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                System.out.println("client receive server request = " + request);
                request.setRemark("client remark");
                return request;
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, Executors.newCachedThreadPool());

        for (int i = 0; i < 3; i++) {
            RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 3);
            System.out.println("invoke result = " + response);
            assertTrue(response != null);
        }
    }

}

class TestRequestHeader implements CommandCustomHeader {
    @CFNullable
    private Integer count;

    @CFNullable
    private String messageTitle;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getMessageTitle() {
        return messageTitle;
    }

    public void setMessageTitle(String messageTitle) {
        this.messageTitle = messageTitle;
    }
}

class TestResponseHeader implements CommandCustomHeader {
    @CFNullable
    private Integer count;

    @CFNullable
    private String messageTitle;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getMessageTitle() {
        return messageTitle;
    }

    public void setMessageTitle(String messageTitle) {
        this.messageTitle = messageTitle;
    }

}
