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
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdaptiveLockTest {

    AdaptiveLockImpl adaptiveLock;

    @Before
    public void init() {
        adaptiveLock = new AdaptiveLockImpl();
    }

    @Test
    public void testAdaptiveLock() throws InterruptedException {
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof CollisionRetreatLock);

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
        assertEquals(2000, ((CollisionRetreatLock) adaptiveLock.getAdaptiveLock()).getOptimalDegree());

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
        assertEquals(4000, ((CollisionRetreatLock) adaptiveLock.getAdaptiveLock()).getOptimalDegree());

        for (int i = 0; i < 3; i++) {
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
        assertEquals(10000, ((CollisionRetreatLock) adaptiveLock.getAdaptiveLock()).getOptimalDegree());


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
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof PutMessageReentrantLock);

        Thread.sleep(1000L);
        adaptiveLock.lock();
        adaptiveLock.unlock();
        Thread.sleep(1000L);
        adaptiveLock.lock();
        adaptiveLock.unlock();
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof CollisionRetreatLock);

        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 100000; i++) {
                        adaptiveLock.lock();
                        adaptiveLock.unlock();
                        if (i == 50000) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                //
                            }
                        }
                    }

                }
            }).start();
        }
        assertTrue(adaptiveLock.getAdaptiveLock() instanceof CollisionRetreatLock);
    }
}
