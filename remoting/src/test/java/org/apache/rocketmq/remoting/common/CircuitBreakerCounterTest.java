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
package org.apache.rocketmq.remoting.common;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CircuitBreakerCounterTest {

    @Test
    void checkAndAddTest() throws InterruptedException {
        int recoverSecond = 1;
        int degradeRuleCount = 3;
        int degradeRuleSecond = 1;
        CircuitBreakerCounter counter = new CircuitBreakerCounter(recoverSecond, degradeRuleCount, degradeRuleSecond);
        counter.start();

        String key = "127.0.0.1";
        int count = 2;
        counter.addCount(key, count);
        assertTrue(counter.check(key));
        counter.addCount(key, count);
        assertFalse(counter.check(key));
        Thread.sleep(2000);
        assertTrue(counter.check(key));
        counter.stop();
    }

    @Test
    void concurrentTest() throws InterruptedException {
        int recoverSecond = 100;
        int degradeRuleCount = 3;
        int degradeRuleSecond = 100;
        int nTreads = 10;
        final String key = "127.0.0.1";
        CircuitBreakerCounter counter = new CircuitBreakerCounter(recoverSecond, degradeRuleCount, degradeRuleSecond);
        counter.start();

        ExecutorService executorService = Executors.newFixedThreadPool(nTreads);
        CountDownLatch countDownLatch = new CountDownLatch(nTreads);
        for (int i = 0; i < nTreads; i++) {
            executorService.submit(() -> {
                counter.addCount(key, 1);
                countDownLatch.countDown();
            });
        }

        countDownLatch.await();
        assertEquals(counter.getCount(key), degradeRuleCount);
        assertFalse(counter.check(key));
    }
}