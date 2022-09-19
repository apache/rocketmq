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
package org.apache.rocketmq.remoting.netty;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class RemotingCodeDistributionHandlerTest {

    private final RemotingCodeDistributionHandler distributionHandler = new RemotingCodeDistributionHandler();

    @Test
    public void remotingCodeTest() throws Exception {
        Class<RemotingCodeDistributionHandler> clazz = RemotingCodeDistributionHandler.class;
        Method methodIn = clazz.getDeclaredMethod("countInbound", int.class);
        Method methodOut = clazz.getDeclaredMethod("countOutbound", int.class);
        methodIn.setAccessible(true);
        methodOut.setAccessible(true);

        int count = 100 * 1000;
        AtomicBoolean result = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(4, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "RemotingCodeTest_" + this.threadIndex.incrementAndGet());
            }
        });

        for (int i = 0; i < count; i++) {
            executorService.submit(() -> {
                try {
                    methodIn.invoke(distributionHandler, 1);
                    methodOut.invoke(distributionHandler, 1);
                } catch (Exception e) {
                    result.set(false);
                }
            });
        }

        Assert.assertTrue(result.get());
        Assert.assertEquals(distributionHandler.getInBoundSnapshotString(), "{1:" + count + "}");
        Assert.assertEquals(distributionHandler.getOutBoundSnapshotString(), "{1:" + count + "}");
    }
}