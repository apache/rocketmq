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

package org.apache.rocketmq.common.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncShutdownHelperLifecycleTest {
    @Test
    public void testEmptyShutdownCanBeAwaited() throws Exception {
        assertTrue(new AsyncShutdownHelper().shutdown().await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testTimeoutDoesNotPreventLaterAwait() throws Exception {
        AsyncShutdownHelper helper = new AsyncShutdownHelper();
        CountDownLatch release = new CountDownLatch(1);
        helper.addTarget(() -> release.await());
        helper.shutdown();
        try {
            assertFalse(helper.await(0, TimeUnit.MILLISECONDS));
        } finally {
            release.countDown();
        }
        assertTrue(helper.await(5, TimeUnit.SECONDS));
        assertTrue(helper.await(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testConcurrentShutdownInvokesEachTargetOnce() throws Exception {
        AsyncShutdownHelper helper = new AsyncShutdownHelper();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        helper.addTarget(() -> {
            calls.incrementAndGet();
            release.await();
        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = executor.submit(() -> helper.shutdown());
            Future<?> second = executor.submit(() -> helper.shutdown());
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
        assertTrue(helper.await(5, TimeUnit.SECONDS));
        assertEquals(1, calls.get());
    }
}
