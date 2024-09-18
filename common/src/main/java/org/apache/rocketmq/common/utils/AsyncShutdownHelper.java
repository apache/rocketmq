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
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncShutdownHelper {
    private final AtomicBoolean shutdown;
    private final List<Shutdown> targetList;

    private CountDownLatch countDownLatch;

    public AsyncShutdownHelper() {
        this.targetList = new ArrayList<>();
        this.shutdown = new AtomicBoolean(false);
    }

    public void addTarget(Shutdown target) {
        if (shutdown.get()) {
            return;
        }
        targetList.add(target);
    }

    public AsyncShutdownHelper shutdown() {
        if (shutdown.get()) {
            return this;
        }
        if (targetList.isEmpty()) {
            return this;
        }
        this.countDownLatch = new CountDownLatch(targetList.size());
        for (Shutdown target : targetList) {
            Runnable runnable = () -> {
                try {
                    target.shutdown();
                } catch (Exception ignored) {

                } finally {
                    countDownLatch.countDown();
                }
            };
            new Thread(runnable).start();
        }
        return this;
    }

    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        if (shutdown.get()) {
            return false;
        }
        try {
            return this.countDownLatch.await(time, unit);
        } finally {
            shutdown.compareAndSet(false, true);
        }
    }
}
