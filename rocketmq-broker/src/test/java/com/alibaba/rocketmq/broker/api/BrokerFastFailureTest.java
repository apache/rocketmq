/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.broker.api;

import com.alibaba.rocketmq.broker.latency.BrokerFastFailure;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.remoting.netty.RequestTask;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

/**
 * BrokerFastFailure Tester.
 *
 * @author shijia.wxr
 */
public class BrokerFastFailureTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: castRunnable(final Runnable runnable)
     */
    @Test
    public void testCastRunnable() throws Exception {
        BlockingQueue<Runnable> sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(100);
        ExecutorService sendMessageExecutor = sendMessageExecutor = new ThreadPoolExecutor(//
                1,//
                1,//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                sendThreadPoolQueue,//
                new ThreadFactoryImpl("SendMessageThread_"));

        RequestTask rt = new RequestTask(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100000000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null, null);
        sendMessageExecutor.submit(rt);
        sendMessageExecutor.submit(rt);
        sendMessageExecutor.submit(rt);
        sendMessageExecutor.submit(rt);
        sendMessageExecutor.submit(rt);

        final Runnable peek = sendThreadPoolQueue.peek();

        final RequestTask requestTask = BrokerFastFailure.castRunnable(peek);

        Assert.assertTrue(requestTask != null);
    }

    /**
     * Method: start()
     */
    @Test
    public void testStart() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: shutdown()
     */
    @Test
    public void testShutdown() throws Exception {
    }


}
