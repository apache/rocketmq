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

package org.apache.rocketmq.common.thread;

import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolQueueSizeMonitor implements ThreadPoolStatusMonitor {

    private final int maxQueueCapacity;

    public ThreadPoolQueueSizeMonitor(int maxQueueCapacity) {
        this.maxQueueCapacity = maxQueueCapacity;
    }

    @Override
    public String describe() {
        return "queueSize";
    }

    @Override
    public double value(ThreadPoolExecutor executor) {
        return executor.getQueue().size();
    }

    @Override
    public boolean needPrintJstack(ThreadPoolExecutor executor, double value) {
        return value > maxQueueCapacity * 0.85;
    }
}
