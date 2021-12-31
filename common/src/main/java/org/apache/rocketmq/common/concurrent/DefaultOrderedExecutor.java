/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this worObject for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY ObjectIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.utils.BitOperationUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;

public class DefaultOrderedExecutor extends AbstractExecutorService implements OrderedExecutor {

    private ThreadLocal<AtomicInteger> countThreadLocal = ThreadLocal.withInitial(new Supplier<AtomicInteger>() {
        @Override
        public AtomicInteger get() {
            return new AtomicInteger(0);
        }
    });

    private final List<ExecutorService> executors;

    public DefaultOrderedExecutor(String processName, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }
        this.executors = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            this.executors.add(Executors.newSingleThreadExecutor(ThreadUtils.newGenericThreadFactory(processName)));
        }
    }

    protected int computeCode(Object... a) {
        if (a == null || a.length == 0) {
            AtomicInteger atomicInteger = countThreadLocal.get();
            int index = atomicInteger.getAndIncrement();
            if (index < 0) {
                index = 0;
                atomicInteger.set(0);
            }
            return index;
        }

        int result = 1;
        for (Object element : a) {
            result = BitOperationUtils.add(BitOperationUtils.multiply(31, result),
                (element == null ? 0 : element.hashCode()));
        }

        return result < 0 ? Math.abs(result) : result;
    }

    protected ExecutorService choose(Object... args) {
        final int size = this.executors.size();
        if (size == 1) {
            return this.executors.get(0);
        }
        return this.executors.get(computeCode(args) % size);
    }

    @Override
    public void shutdown() {
        for (ExecutorService executor : this.executors) {
            ThreadUtils.shutdownGracefully(executor, 1000, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        return this.executors.stream()
            .map(ExecutorService::shutdownNow)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    @Override
    public boolean isShutdown() {
        boolean result = true;
        for (ExecutorService executor : this.executors) {
            result = result && executor.isShutdown();
        }
        return result;
    }

    @Override
    public boolean isTerminated() {
        boolean result = true;
        for (ExecutorService executor : this.executors) {
            result = result && executor.isTerminated();
        }
        return result;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = true;
        for (ExecutorService executor : this.executors) {
            result = result && executor.awaitTermination(timeout, unit);
        }
        return result;
    }

    @Override
    public void execute(Runnable command, Object... keys) {
        choose(keys).execute(command);
    }

    @Override
    public void execute(Runnable command) {
        choose().execute(command);
    }

    public static void main(String[] args) {
        DefaultOrderedExecutor executor = new DefaultOrderedExecutorService("test", 10);
        for (int i = 0; i < 100; i++) {
            System.out.println(executor.computeCode());
        }
        System.out.println();
        for (int i = 0; i < 100; i++) {
            System.out.println(executor.computeCode((Object[]) null));
        }
    }
}
