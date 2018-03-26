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

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class PutMessageSpinLockTest {

    class HelperRunnable implements Runnable {
        PutMessageSpinLock lock;
        volatile boolean canRelease = false;

        HelperRunnable(PutMessageSpinLock lock) {
            this.lock = lock;
        }

        public void run() {
            lock.lock();
            while (!canRelease) {
                Thread.yield();
            }
            lock.unlock();
        }

        public void fireRelease() {
            canRelease = true;
        }
    }

    void awaitTermination(Thread t, long timeoutMillis) {
        try {
            t.join(timeoutMillis);
        } catch (InterruptedException fail) {
        }
    }

    /**
     * Spin-waits until sync.isQueued(t) becomes true.
     */
    void waitForQueuedThread(PutMessageSpinLock lock, Thread t) {
        while (!lock.isQueued(t)) {
            Thread.yield();
        }
        assertTrue(t.isAlive());
    }

    /**
     * Spin-waits until sync.isQueued(t) becomes false.
     */
    void waitForOutQueuedThread(PutMessageSpinLock lock, Thread t) {
        while (lock.isQueued(t)) {
            Thread.yield();
        }
    }

    @Test
    public void testIsLocked() {
        PutMessageSpinLock lock = new PutMessageSpinLock();
        assertFalse(lock.isLocked());
    }

    @Test
    public void testAcquire() {
        PutMessageSpinLock lock = new PutMessageSpinLock();
        lock.lock();
        assertTrue(lock.isLocked());
        lock.unlock();
        assertFalse(lock.isLocked());
    }

    @Test
    public void testHasQueuedThreads() {
        PutMessageSpinLock lock = new PutMessageSpinLock();
        assertFalse(lock.hasQueuedThreads());
        lock.lock();
        HelperRunnable r1 = new HelperRunnable(lock);
        Thread t1 = new Thread(r1);
        t1.start();
        waitForQueuedThread(lock, t1);
        assertTrue(lock.hasQueuedThreads());
        HelperRunnable r2 = new HelperRunnable(lock);
        Thread t2 = new Thread(r2);
        t2.start();
        waitForQueuedThread(lock, t2);
        assertTrue(lock.hasQueuedThreads());
        lock.unlock();
        waitForOutQueuedThread(lock, t1);
        assertTrue(lock.hasQueuedThreads());
        r1.fireRelease();
        awaitTermination(t1, 100);
        waitForOutQueuedThread(lock, t2);
        r2.fireRelease();
        awaitTermination(t2, 100);
        assertFalse(lock.hasQueuedThreads());
    }

    @Test
    public void testIsQueued() {
        PutMessageSpinLock lock = new PutMessageSpinLock();
        HelperRunnable r1 = new HelperRunnable(lock);
        Thread t1 = new Thread(r1);
        HelperRunnable r2 = new HelperRunnable(lock);
        Thread t2 = new Thread(r2);
        assertFalse(lock.isQueued(t1));
        assertFalse(lock.isQueued(t2));
        lock.lock();
        t1.start();
        waitForQueuedThread(lock, t1);
        assertTrue(lock.isQueued(t1));
        assertFalse(lock.isQueued(t2));
        t2.start();
        waitForQueuedThread(lock, t2);
        assertTrue(lock.isQueued(t1));
        assertTrue(lock.isQueued(t2));
        lock.unlock();
        waitForOutQueuedThread(lock, t1);
        assertFalse(lock.isQueued(t1));
        assertTrue(lock.isQueued(t2));
        r1.fireRelease();
        awaitTermination(t1, 100);
        waitForOutQueuedThread(lock, t2);
        assertFalse(lock.isQueued(t1));
        assertFalse(lock.isQueued(t2));
        r2.fireRelease();
        awaitTermination(t2, 100);
    }

    @Test
    public void testHasContended() {
        PutMessageSpinLock lock = new PutMessageSpinLock();
        assertFalse(lock.hasContended());
        lock.lock();
        assertFalse(lock.hasContended());
        HelperRunnable r1 = new HelperRunnable(lock);
        Thread t1 = new Thread(r1);
        t1.start();
        waitForQueuedThread(lock, t1);
        assertTrue(lock.hasContended());
        HelperRunnable r2 = new HelperRunnable(lock);
        Thread t2 = new Thread(r2);
        t2.start();
        waitForQueuedThread(lock, t2);
        assertTrue(lock.hasContended());
        lock.unlock();
        waitForOutQueuedThread(lock, t1);
        r1.fireRelease();
        awaitTermination(t1, 100);
        assertTrue(lock.hasContended());
        waitForOutQueuedThread(lock, t2);
        r2.fireRelease();
        awaitTermination(t2, 100);
        assertTrue(lock.hasContended());
    }
}