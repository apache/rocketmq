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

import org.apache.rocketmq.store.PutMessageReentrantLock;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdaptiveLockTest {

    AdaptiveBackOffSpinLockImpl adaptiveLock;

    @Before
    public void init() {
        adaptiveLock = new AdaptiveBackOffSpinLockImpl(new PutMessageSpinLock());
    }

    @Test
    public void testAdaptiveLock() throws InterruptedException {
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof BackOffSpinLock);

        adaptiveLock.lock();
        new Thread(new Runnable() {
            @Override
            public void run() {
                adaptiveLock.lock();
                adaptiveLock.unlock();
            }
        }).start();
        Thread.sleep(1000);
        adaptiveLock.unlock();
        assertEquals(2000, ((BackOffSpinLock) adaptiveLock.getAdaptiveLock()).getOptimalDegree());



        for (int i = 0; i <= 8; i++) {
            adaptiveLock.lock();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    adaptiveLock.lock();
                    adaptiveLock.unlock();
                }
            }).start();
            Thread.sleep(1000);
            adaptiveLock.unlock();
        }
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof BackOffReentrantLock);

        adaptiveLock.lock();
        Thread.sleep(1000);
        adaptiveLock.unlock();
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof BackOffSpinLock);
    }
}
