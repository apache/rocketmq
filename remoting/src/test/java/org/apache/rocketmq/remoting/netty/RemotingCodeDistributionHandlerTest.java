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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class RemotingCodeDistributionHandlerTest {

    private final RemotingCodeDistributionHandler distributionHandler = new RemotingCodeDistributionHandler();

    @Test
    public void remotingCodeCountTest() throws Exception {
        Class<RemotingCodeDistributionHandler> clazz = RemotingCodeDistributionHandler.class;
        Method methodIn = clazz.getDeclaredMethod("countInbound", int.class);
        Method methodOut = clazz.getDeclaredMethod("countOutbound", int.class);
        methodIn.setAccessible(true);
        methodOut.setAccessible(true);

        int threadCount = 4;
        int count = 1000 * 1000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicBoolean result = new AtomicBoolean(true);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, new ThreadFactoryImpl("RemotingCodeTest_"));

        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < count; j++) {
                        methodIn.invoke(distributionHandler, 1);
                        methodOut.invoke(distributionHandler, 2);
                    }
                } catch (Exception e) {
                    result.set(false);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        Assert.assertTrue(result.get());
        await().pollInterval(Duration.ofMillis(100)).atMost(Duration.ofSeconds(10)).until(() -> {
            boolean f1 = ("{1:" + count * threadCount + "}").equals(distributionHandler.getInBoundSnapshotString());
            boolean f2 = ("{2:" + count * threadCount + "}").equals(distributionHandler.getOutBoundSnapshotString());
            return f1 && f2;
        });
    }
}