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
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**

 *
 * @author shijia.wxr
 *
 */
public class NettyConnectionTest {
    @Test
    public void test_connect_timeout() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 100; i++) {
            try {
                RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
                RemotingCommand response = client.invokeSync("localhost:8888", request, 1000 * 3);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        client.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }


    @Test
    public void test_async_timeout() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RemotingClient client = createRemotingClient();
        final AtomicInteger ai = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(100);
        for(int i=0;i<100;i++) {
            try {
                RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
                client.invokeAsync("localhost:8888", request, 5, new InvokeCallback() {//very easy to timeout
                    @Override
                    public void operationComplete(ResponseFuture responseFuture) {
                        if (responseFuture.isTimeout()) {
                            if(ai.getAndIncrement()==4) {
                                try {
                                    System.out.println("First try timeout,  blocking 10s" + Thread.currentThread().getName());
                                    Thread.sleep(10 * 1000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            else{
                                System.out.println("Timeout callback execute,very short."+Thread.currentThread().getName());
                            }
                        }
                        else{
                            System.out.println("Success."+Thread.currentThread().getName());
                        }
                        latch.countDown();

                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }



        latch.await(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, latch.getCount());//only one should be blocked
        client.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }

    public static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(15);
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }
}
