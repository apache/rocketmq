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
package org.apache.rocketmq.common.concurrent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultOrderedExecutorServiceTest {

    static OrderedExecutorService executor = new DefaultOrderedExecutorService("test", 7);

    @After
    public void shutdown() throws Exception {
        executor.shutdown();
        assertThat(executor.shutdownNow()).isEmpty();
        assertThat(executor.isShutdown()).isTrue();
        assertThat(executor.isTerminated()).isTrue();
        assertThat(executor.isShutdown()).isTrue();
    }

    @Test
    public void testFrom() {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(7);
        OrderedExecutorService from = OrderedExecutorService.from("test", pool);
        assertThat(pool.getCorePoolSize()).isEqualTo(from.getCorePoolSize());
    }

    @Test
    public void testIllegalSize() {
        try {
            new DefaultOrderedExecutorService("test", 0);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testRoundRibbon() throws Exception {
        for (int i = 0; i < 100000; i++) {
            int code = executor.computeCode();
            assertThat(code).isEqualTo(i);
        }

        //overflow
        executor.setCode(Integer.MAX_VALUE);
        executor.computeCode();
        int code = executor.computeCode();
        assertThat(code).isEqualTo(0);

        CountDownLatch count = new CountDownLatch(100000);
        Map<Thread, Integer> map = Collections.synchronizedMap(new HashMap<>());
        for (int i = 0; i < 100000; i++) {
            executor.execute(() -> {
                final Thread thread = Thread.currentThread();
                Integer integer = map.get(thread);
                if (null == integer) {
                    integer = 0;
                    map.put(thread, integer);
                } else {
                    map.put(thread, ++integer);
                }
                count.countDown();
            });
        }
        count.await();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (Integer value : map.values()) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        assertThat(max - 1).isEqualTo(min);
    }

    @Test
    public void testHash() throws Exception {
        int code = executor.computeCode("topic");
        for (int i = 0; i < 1000; i++) {
            assertThat(executor.computeCode("topic")).isEqualTo(code);
        }
        code = executor.computeCode("topic", 1);
        for (int i = 0; i < 1000; i++) {
            assertThat(executor.computeCode("topic", 1)).isEqualTo(code);
        }

        OrderedExecutorService executor = new DefaultOrderedExecutorService("test", 7);
        CountDownLatch count = new CountDownLatch(100000);
        Map<Thread, Integer> map = Collections.synchronizedMap(new HashMap<>());
        Runnable task = () -> {
            final Thread thread = Thread.currentThread();
            Integer integer = map.get(thread);
            if (null == integer) {
                integer = 0;
                map.put(thread, integer);
            } else {
                map.put(thread, ++integer);
            }
            count.countDown();
        };
        for (int i = 0; i < 100000; i++) {
            executor.execute(task, 1);
            executor.submit(task, new Object[] {2});
            executor.submit(() -> {
                task.run();
                return null;
            }, 3);
            executor.submit(task, 4, new Object[] {4});
        }
        count.await();
        executor.shutdown();
        assertThat(map.size()).isEqualTo(4);
    }
}
