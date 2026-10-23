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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SemaphoreReleaseOnlyOnceTest {

    @Test
    public void testReleaseReleasesExactlyOnePermit() {
        Semaphore semaphore = new Semaphore(0);
        SemaphoreReleaseOnlyOnce onlyOnce = new SemaphoreReleaseOnlyOnce(semaphore);

        onlyOnce.release();
        assertThat(semaphore.availablePermits()).isEqualTo(1);

        // repeated releases by the same holder must not add more permits
        onlyOnce.release();
        onlyOnce.release();
        assertThat(semaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testConcurrentReleaseAddsExactlyOnePermit() throws Exception {
        final int threads = 8;
        Semaphore semaphore = new Semaphore(0);
        SemaphoreReleaseOnlyOnce onlyOnce = new SemaphoreReleaseOnlyOnce(semaphore);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        final AtomicInteger failures = new AtomicInteger(0);

        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            workers[i] = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
                onlyOnce.release();
            });
            workers[i].start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(failures.get()).isEqualTo(0);
        // however many threads race on release(), the semaphore gains exactly one permit
        assertThat(semaphore.availablePermits()).isEqualTo(1);
    }

    @Test
    public void testNullSemaphoreIsIgnored() {
        SemaphoreReleaseOnlyOnce onlyOnce = new SemaphoreReleaseOnlyOnce(null);
        // must be a silent no-op, not an NPE
        onlyOnce.release();
        onlyOnce.release();
        assertThat(onlyOnce.getSemaphore()).isNull();
    }
}
