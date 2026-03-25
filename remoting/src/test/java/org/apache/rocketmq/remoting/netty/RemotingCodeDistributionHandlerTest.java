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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemotingCodeDistributionHandlerTest {

    private RemotingCodeDistributionHandler handler;

    @Before
    public void setUp() {
        handler = new RemotingCodeDistributionHandler();
    }

    @Test
    public void testInboundCountAndTraffic() {
        handler.recordInbound(100, 512);
        handler.recordInbound(100, 1024);

        Assert.assertEquals("{100:2}", handler.getInBoundSnapshotString());
        Assert.assertEquals("{100:1536}", handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testOutboundCountAndTraffic() {
        handler.recordOutbound(0, 256);
        handler.recordOutbound(0, 256);
        handler.recordOutbound(0, 512);

        Assert.assertEquals("{0:3}", handler.getOutBoundSnapshotString());
        Assert.assertEquals("{0:1024}", handler.getOutBoundTrafficSnapshotString());
    }

    @Test
    public void testMultipleRequestCodes() {
        handler.recordInbound(10, 200);
        handler.recordInbound(10, 200);
        handler.recordInbound(20, 300);

        String countSnapshot = handler.getInBoundSnapshotString();
        Assert.assertNotNull(countSnapshot);
        Assert.assertTrue(countSnapshot.contains("10:2"));
        Assert.assertTrue(countSnapshot.contains("20:1"));

        String trafficSnapshot = handler.getInBoundTrafficSnapshotString();
        Assert.assertNotNull(trafficSnapshot);
        Assert.assertTrue(trafficSnapshot.contains("10:400"));
        Assert.assertTrue(trafficSnapshot.contains("20:300"));
    }

    @Test
    public void testSnapshotResetsAfterRead() {
        handler.recordInbound(400, 100);

        Assert.assertNotNull(handler.getInBoundSnapshotString());
        Assert.assertNotNull(handler.getInBoundTrafficSnapshotString());

        // Second read returns null after sumThenReset
        Assert.assertNull(handler.getInBoundSnapshotString());
        Assert.assertNull(handler.getInBoundTrafficSnapshotString());
    }

    @Test
    public void testEmptySnapshotReturnsNull() {
        Assert.assertNull(handler.getInBoundSnapshotString());
        Assert.assertNull(handler.getOutBoundSnapshotString());
        Assert.assertNull(handler.getInBoundTrafficSnapshotString());
        Assert.assertNull(handler.getOutBoundTrafficSnapshotString());
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        int threadCount = 4;
        int countPerThread = 100_000;
        int wireSize = 512;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicBoolean success = new AtomicBoolean(true);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < countPerThread; j++) {
                        handler.recordInbound(1, wireSize);
                    }
                } catch (Exception e) {
                    success.set(false);
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        Assert.assertTrue(success.get());

        long totalCount = threadCount * (long) countPerThread;
        long totalTraffic = totalCount * wireSize;
        Assert.assertEquals("{1:" + totalCount + "}", handler.getInBoundSnapshotString());
        Assert.assertEquals("{1:" + totalTraffic + "}", handler.getInBoundTrafficSnapshotString());

        executor.shutdown();
    }
}
