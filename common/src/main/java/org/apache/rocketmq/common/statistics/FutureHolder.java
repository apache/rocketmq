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
package org.apache.rocketmq.common.statistics;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class FutureHolder<T> {
    private final ConcurrentMap<T, BlockingQueue<Future>> futureMap = new ConcurrentHashMap<>(8);

    public void addFuture(T key, Future future) {
        futureMap.computeIfAbsent(key, k -> new LinkedBlockingQueue<>()).add(future);
    }

    public void removeAllFuture(T key) {
        cancelAll(key, false);
        futureMap.remove(key);
    }

    private void cancelAll(T key, boolean mayInterruptIfRunning) {
        BlockingQueue<Future> list = futureMap.get(key);
        if (list != null) {
            list.forEach(future -> future.cancel(mayInterruptIfRunning));
        }
    }
}
