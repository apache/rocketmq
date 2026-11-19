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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThreadUtilsShutdownTest {
    @Test(timeout = 10000)
    public void testFiniteTimeoutDoesNotWaitForever() throws Exception {
        assertShutdownReturns(false);
    }

    @Test(timeout = 10000)
    public void testInterruptedCallerReturnsWithInterruptPreserved() throws Exception {
        assertShutdownReturns(true);
    }

    private void assertShutdownReturns(boolean interruptCaller) throws Exception {
        // Initialize the logger before measuring an interruptible join on a cold JVM.
        ThreadUtils.shutdownGracefully((Thread) null, 0);
        CountDownLatch release = new CountDownLatch(1);
        Thread worker = new Thread(() -> {
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // Simulate a worker that cannot stop until its operation completes.
                }
            }
        });
        worker.setDaemon(true);
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        CountDownLatch returned = new CountDownLatch(1);
        Thread caller = new Thread(() -> {
            if (interruptCaller) {
                Thread.currentThread().interrupt();
            }
            ThreadUtils.shutdownGracefully(worker, interruptCaller ? 0 : 50);
            interruptPreserved.set(Thread.currentThread().isInterrupted());
            returned.countDown();
        });
        caller.setDaemon(true);
        worker.start();
        caller.start();
        try {
            assertTrue("Shutdown must honor timeout or caller interruption",
                returned.await(5, TimeUnit.SECONDS));
            assertTrue(worker.isAlive());
            if (interruptCaller) {
                assertTrue(interruptPreserved.get());
            } else {
                assertFalse(interruptPreserved.get());
            }
        } finally {
            release.countDown();
            worker.join(1000);
            caller.join(1000);
        }
    }
}
