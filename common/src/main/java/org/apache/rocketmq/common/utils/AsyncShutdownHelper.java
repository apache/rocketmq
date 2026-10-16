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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncShutdownHelper {
    private boolean shutdown;
    private final List<Shutdown> targetList;

    private volatile CountDownLatch countDownLatch;

    public AsyncShutdownHelper() {
        this.targetList = new ArrayList<>();
        this.shutdown = false;
    }

    public synchronized void addTarget(Shutdown target) {
        if (shutdown) {
            return;
        }
        targetList.add(target);
    }

    public synchronized AsyncShutdownHelper shutdown() {
        if (shutdown) {
            return this;
        }
        shutdown = true;
        final CountDownLatch latch = new CountDownLatch(targetList.size());
        this.countDownLatch = latch;
        for (Shutdown target : targetList) {
            Runnable runnable = () -> {
                try {
                    target.shutdown();
                } catch (Exception ignored) {

                } finally {
                    latch.countDown();
                }
            };
            new Thread(runnable).start();
        }
        return this;
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        CountDownLatch latch = this.countDownLatch;
        if (latch == null) {
            throw new IllegalStateException("shutdown has not been started");
        }
        return latch.await(time, unit);
    }
}
