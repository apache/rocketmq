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

package org.apache.rocketmq.store.ha;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class WaitNotifyObjectTest {
    @Test
    public void removeFromWaitingThreadTable() throws Exception {
        final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
        for (int i = 0; i < 5; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    waitNotifyObject.allWaitForRunning(100);
                    waitNotifyObject.removeFromWaitingThreadTable();
                }
            });
            t.start();
            t.join();
        }
        Assert.assertEquals(0, waitNotifyObject.waitingThreadTable.size());
    }

    @Test
    public void allWaitForRunning() throws Exception {

        final int threadNum = 5;
        final long waitIntervalMs = 100L;
        final CountDownLatch latch = new CountDownLatch(threadNum);
        final WaitNotifyObject waitNotifyObject = new WaitNotifyObject() {
            @Override
            protected void onWaitEnd() {
                latch.countDown();
            }
        };
        long start = System.nanoTime();
        for (int i = 0; i < threadNum; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    waitNotifyObject.allWaitForRunning(waitIntervalMs);
                }
            });
            t.start();
            t.join();
        }

        latch.await();
        long elapsed = (System.nanoTime() - start) / 1000000;
        Assert.assertEquals(threadNum, waitNotifyObject.waitingThreadTable.size());
        Assert.assertTrue(elapsed >= threadNum * waitIntervalMs);
    }

    @Test
    public void wakeup() throws Exception {
        final long waitIntervalMs = 3000L;
        final long sleepMs = 500L;
        final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
        long start = System.nanoTime();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(sleepMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                waitNotifyObject.wakeup();
            }
        });
        t.start();
        waitNotifyObject.waitForRunning(waitIntervalMs);
        long elapsed = (System.nanoTime() - start) / 1000000;
        Assert.assertTrue(elapsed >= sleepMs && elapsed < waitIntervalMs);
    }

}
