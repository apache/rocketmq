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
package org.apache.rocketmq.store;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;

public class StoreStatsServiceTest {

    @Test
    public void getSinglePutMessageTopicSizeTotal() throws Exception {
        final StoreStatsService storeStatsService = new StoreStatsService();
        int num = Runtime.getRuntime().availableProcessors() * 2;
        for (int j = 0; j < 100; j++) {
            final AtomicReference<LongAdder> reference = new AtomicReference<>(null);
            final CountDownLatch latch = new CountDownLatch(num);
            final CyclicBarrier barrier = new CyclicBarrier(num);
            for (int i = 0; i < num; i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            barrier.await();
                            LongAdder longAdder = storeStatsService.getSinglePutMessageTopicSizeTotal("test");
                            if (reference.compareAndSet(null, longAdder)) {
                            } else if (reference.get() != longAdder) {
                                throw new RuntimeException("Reference should be same!");
                            }
                        } catch (InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }
                }).start();
            }
            latch.await();
        }
    }

    @Test
    public void getSinglePutMessageTopicTimesTotal() throws Exception {
        final StoreStatsService storeStatsService = new StoreStatsService();
        int num = Runtime.getRuntime().availableProcessors() * 2;
        for (int j = 0; j < 100; j++) {
            final AtomicReference<LongAdder> reference = new AtomicReference<>(null);
            final CountDownLatch latch = new CountDownLatch(num);
            final CyclicBarrier barrier = new CyclicBarrier(num);
            for (int i = 0; i < num; i++) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            barrier.await();
                            LongAdder longAdder = storeStatsService.getSinglePutMessageTopicTimesTotal("test");
                            if (reference.compareAndSet(null, longAdder)) {
                            } else if (reference.get() != longAdder) {
                                throw new RuntimeException("Reference should be same!");
                            }
                        } catch (InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }
                }).start();
            }
            latch.await();
        }
    }

}