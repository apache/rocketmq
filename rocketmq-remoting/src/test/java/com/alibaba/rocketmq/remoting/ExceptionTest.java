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

/**
 * $Id: ExceptionTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.*;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;


/**
 * @author shijia.wxr
 */
public class ExceptionTest {
    private static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        RemotingServer client = new NettyRemotingServer(config);
        client.registerProcessor(0, new NettyRequestProcessor() {
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
        client.start();
        return client;
    }

    @Test
    public void test_CONNECT_EXCEPTION() {
        RemotingClient client = createRemotingClient();

        RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
        RemotingCommand response = null;
        try {
            response = client.invokeSync("localhost:8888", request, 1000 * 3);
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("invoke result = " + response);
        assertTrue(null == response);

        client.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }

    private static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }

}
