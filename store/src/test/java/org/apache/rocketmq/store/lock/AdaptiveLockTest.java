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
package org.apache.rocketmq.store.lock;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdaptiveLockTest {

    AdaptiveBackOffSpinLockImpl adaptiveLock;

    @Before
    public void init() {
        adaptiveLock = new AdaptiveBackOffSpinLockImpl();
    }

    @Test
    public void testAdaptiveLock() throws InterruptedException {
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof BackOffSpinLock);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        adaptiveLock.lock();
        new Thread(new Runnable() {
            @Override
            public void run() {
                adaptiveLock.lock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    //ignore
                }
                adaptiveLock.unlock();
                countDownLatch.countDown();
            }
        }).start();
        Thread.sleep(1000);
        adaptiveLock.unlock();
        assertEquals(2000, ((BackOffSpinLock) adaptiveLock.getAdaptiveLock()).getOptimalDegree());
        countDownLatch.await();

        for (int i = 0; i <= 5; i++) {
            CountDownLatch countDownLatch1 = new CountDownLatch(1);
            adaptiveLock.lock();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    adaptiveLock.lock();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                    adaptiveLock.unlock();
                    countDownLatch1.countDown();
                }
            }).start();
            Thread.sleep(1000);
            adaptiveLock.unlock();
            countDownLatch1.await();
        }
        assertEquals(4, adaptiveLock.getSwapCriticalPoint());
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof BackOffReentrantLock);

        adaptiveLock.lock();
        Thread.sleep(1000);
        adaptiveLock.unlock();
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof BackOffSpinLock);
    }
}
