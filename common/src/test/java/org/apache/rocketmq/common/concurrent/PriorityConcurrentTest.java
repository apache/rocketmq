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

import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class PriorityConcurrentTest {

    @Test
    public void testPriorityConcurrent() {
        PriorityConcurrentEngine.runPriorityAsync(PriorityConcurrentEngine.MAX_PRIORITY,
            () -> System.out.println("hello"));
        AtomicInteger count = new AtomicInteger(0);
        List<Integer> list = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 50; i++) {
            int priority = i;
            for (int j = 0; j < i; j++) {
                PriorityConcurrentEngine.supplyPriorityAsync(priority, new CallableSupplier<Integer>() {
                    @Override
                    public Integer get() {
                        try {
                            Thread.sleep(new Random().nextInt(10));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println(priority);
                        return priority;
                    }

                    @Override
                    public Callback<Integer> getCallback() {
                        return list::add;
                    }
                });
                count.getAndIncrement();
            }
        }
        PriorityConcurrentEngine.runPriorityAsync(() -> {
            List<Integer> copy = new CopyOnWriteArrayList<>(list);
            copy.sort(Integer::compareTo);
            System.out.println(copy.equals(list));
            System.out.println(list.size());
            System.out.println(count.get());
            System.out.println(list);
        });
        PriorityConcurrentEngine.invokeAllNow();
    }
}
