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

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.ServiceThread;

public class FixedSizeCache<K, V> {
    private final CacheCleanService cleanService;
    private final ConcurrentMap<K, V> data;
    private final Queue<K> writeBuffer;
    private final int maxCapacity;
    private final AtomicLong currentCapacity;

    public FixedSizeCache(int maxCapacity) {
        this.data = new ConcurrentHashMap(maxCapacity);
        this.writeBuffer = new ConcurrentLinkedQueue<K>();
        this.maxCapacity = maxCapacity;
        this.currentCapacity = new AtomicLong(0);

        this.cleanService = new CacheCleanService();
        this.cleanService.start();
    }

    public void shutdown() {
        if (this.cleanService != null) {
            this.cleanService.shutdown();
        }
    }

    public void putIfAbsent(final K k, final V v) {
        final V prior = data.putIfAbsent(k, v);
        if (prior == null) {
            this.writeBuffer.add(k);
            this.currentCapacity.incrementAndGet();
        }
    }

    public V get(Object key) {
        return data.get(key);
    }

    private boolean remove(final K k) {
        return this.data.remove(k) != null;
    }

    public void addAndGet(long delta) {
        this.currentCapacity.addAndGet(delta);
    }

    class CacheCleanService extends ServiceThread {

        public void run() {
            while (!this.isStopped()) {
                try {
                    long runTasks = 0;
                    try {
                        while (currentCapacity.get() > maxCapacity) {
                            final K k = writeBuffer.poll();
                            if (k == null) {
                                break;
                            }
                            if (remove(k)) {
                                runTasks++;
                            }
                            if ((runTasks & 0x3F) == 0) {
                                if (this.isStopped()) {
                                    return;
                                }
                            }
                        }
                    } finally {
                        addAndGet(-runTasks);
                    }
                    Thread.sleep(1);
                } catch (Exception ignored) {
                }
            }
        }

        @Override
        public String getServiceName() {
            return CacheCleanService.class.getSimpleName();
        }
    }
}
