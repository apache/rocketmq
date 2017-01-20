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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.util.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelTaskExecutor {
    public List<ParallelTask> tasks = new ArrayList<ParallelTask>();
    public ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    public CountDownLatch latch = null;

    public ParallelTaskExecutor() {

    }

    public void pushTask(ParallelTask task) {
        tasks.add(task);
    }

    public void startBlock() {
        init();
        startTask();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void startNoBlock() {
        for (ParallelTask task : tasks) {
            cachedThreadPool.execute(task);
        }
    }

    private void init() {
        latch = new CountDownLatch(tasks.size());
        for (ParallelTask task : tasks) {
            task.setLatch(latch);
        }
    }

    private void startTask() {
        for (ParallelTask task : tasks) {
            task.start();
        }
    }
}
