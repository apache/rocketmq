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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class ConcurrentEngine {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    protected static ThreadPoolExecutor enginePool = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);

    public static ThreadPoolExecutor getEnginePool() {
        return enginePool;
    }

    public static void setEnginePool(ThreadPoolExecutor enginePool) {
        if (enginePool == null) {
            throw new RuntimeException("enginePool can not set to null !");
        }
        ConcurrentEngine.enginePool = enginePool;
    }

    public static void setEnginePoolCoreSize(int corePoolSize) {
        if (corePoolSize > 0 && corePoolSize <= Short.MAX_VALUE) {
            ConcurrentEngine.enginePool.setCorePoolSize(corePoolSize);
        }
    }

    public static void runAsync(Runnable... tasks) {
        runAsync(UtilAll.newArrayList(tasks));
    }

    protected static <E> List<E> pollAllTask(Queue<E> tasks) {
        List<E> list = new ArrayList<>();
        while (tasks != null && !tasks.isEmpty()) {
            E task = tasks.poll();
            list.add(task);
        }
        return list;
    }

    protected static <T> void doCallback(CallableSupplier<T> supplier, T response) {
        Collection<Callback<T>> callbacks = supplier.getCallbacks();
        if (CollectionUtils.isNotEmpty(callbacks)) {
            for (Callback<T> callback : callbacks) {
                callback.call(response);
            }
        }
    }

    public static void runAsync(Queue<Runnable> tasks) {
        runAsync(pollAllTask(tasks));
    }

    public static void runAsync(Collection<Runnable> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        List<CompletableFuture<Void>> list = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            list.add(CompletableFuture.runAsync(task, enginePool));
        }
        executeAsync(list);
    }

    @SafeVarargs
    public static <T> List<T> supplyAsync(Supplier<T>... tasks) {
        return supplyAsync(UtilAll.newArrayList(tasks));
    }

    public static <T> List<T> supplyAsync(Queue<Supplier<T>> tasks) {
        return supplyAsync(pollAllTask(tasks));
    }

    public static <T> List<T> supplyAsync(Collection<Supplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        List<CompletableFuture<T>> list = new ArrayList<>(tasks.size());
        for (Supplier<T> task : tasks) {
            list.add(CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeAsync(list);
    }

    @SafeVarargs
    public static <T> List<T> supplyCallableAsync(CallableSupplier<T>... tasks) {
        return supplyCallableAsync(UtilAll.newArrayList(tasks));
    }

    public static <T> List<T> supplyCallableAsync(Queue<CallableSupplier<T>> tasks) {
        return supplyCallableAsync(pollAllTask(tasks));
    }

    public static <T> List<T> supplyCallableAsync(Collection<CallableSupplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        Map<CallableSupplier<T>, CompletableFuture<T>> map = new HashMap<>(tasks.size());
        for (CallableSupplier<T> task : tasks) {
            map.put(task, CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<CallableSupplier<T>, T> result = executeKeyedAsync(map);
        for (Map.Entry<CallableSupplier<T>, T> entry : result.entrySet()) {
            doCallback(entry.getKey(), entry.getValue());
        }
        return UtilAll.newArrayList(result.values());
    }

    @SafeVarargs
    public static <K, V> Map<K, V> supplyKeyedCallableAsync(KeyedCallableSupplier<K, V>... tasks) {
        return supplyKeyedCallableAsync(UtilAll.newArrayList(tasks));
    }

    public static <K, V> Map<K, V> supplyKeyedCallableAsync(Queue<KeyedCallableSupplier<K, V>> tasks) {
        return supplyKeyedCallableAsync(pollAllTask(tasks));
    }

    public static <K, V> Map<K, V> supplyKeyedCallableAsync(Collection<KeyedCallableSupplier<K, V>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new HashMap<>();
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedCallableSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<K, V> result = executeKeyedAsync(map);
        for (KeyedCallableSupplier<K, V> task : tasks) {
            K key = task.key();
            V response = result.get(key);
            doCallback(task, response);
        }
        return result;
    }

    @SafeVarargs
    public static <T> List<T> executeAsync(CompletableFuture<T>... tasks) {
        return executeAsync(UtilAll.newArrayList(tasks));
    }

    public static <T> List<T> executeAsync(Queue<CompletableFuture<T>> tasks) {
        return executeAsync(pollAllTask(tasks));
    }

    public static <T> List<T> executeAsync(Collection<CompletableFuture<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getResultIgnoreException(tasks);
    }

    public static <T> List<T> getResultIgnoreException(Collection<CompletableFuture<T>> tasks) {
        List<T> result = new ArrayList<>(tasks.size());
        for (CompletableFuture<T> completableFuture : tasks) {
            if (null == completableFuture) {
                continue;
            }
            try {
                T response = completableFuture.get();
                if (null != response) {
                    result.add(response);
                }
            } catch (Exception e) {
                log.error("task:{} execute failed with exception:{}", completableFuture, e.getMessage(), e);
            }
        }
        return result;
    }

    public static void runAsync(long timeout, TimeUnit unit, Runnable... tasks) {
        runAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public static void runAsync(long timeout, TimeUnit unit, Queue<Runnable> tasks) {
        runAsync(timeout, unit, pollAllTask(tasks));
    }

    public static void runAsync(long timeout, TimeUnit unit, Collection<Runnable> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        List<CompletableFuture<Void>> list = new ArrayList<>(tasks.size());
        for (Runnable task : tasks) {
            list.add(CompletableFuture.runAsync(task, enginePool));
        }
        executeAsync(timeout, unit, list);
    }

    @SafeVarargs
    public static <T> List<T> supplyAsync(long timeout, TimeUnit unit, Supplier<T>... tasks) {
        return supplyAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public static <T> List<T> supplyAsync(long timeout, TimeUnit unit, Queue<Supplier<T>> tasks) {
        return supplyAsync(timeout, unit, pollAllTask(tasks));
    }

    public static <T> List<T> supplyAsync(long timeout, TimeUnit unit, Collection<Supplier<T>> tasks) {
        if (null == tasks || tasks.size() == 0) {
            return new ArrayList<>();
        }
        List<CompletableFuture<T>> list = new ArrayList<>(tasks.size());
        for (Supplier<T> task : tasks) {
            list.add(CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeAsync(timeout, unit, list);
    }

    @SafeVarargs
    public static <T> List<T> supplyCallableAsync(long timeout, TimeUnit unit, CallableSupplier<T>... tasks) {
        return supplyCallableAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public static <T> List<T> supplyCallableAsync(long timeout, TimeUnit unit, Queue<CallableSupplier<T>> tasks) {
        return supplyCallableAsync(timeout, unit, pollAllTask(tasks));
    }

    public static <T> List<T> supplyCallableAsync(long timeout, TimeUnit unit, Collection<CallableSupplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        Map<CallableSupplier<T>, CompletableFuture<T>> map = new HashMap<>(tasks.size());
        for (CallableSupplier<T> task : tasks) {
            map.put(task, CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<CallableSupplier<T>, T> result = executeKeyedAsync(map, timeout, unit);
        for (Map.Entry<CallableSupplier<T>, T> entry : result.entrySet()) {
            doCallback(entry.getKey(), entry.getValue());
        }
        return UtilAll.newArrayList(result.values());
    }

    @SafeVarargs
    public static <K, V> Map<K, V> supplyKeyedCallableAsync(long timeout, TimeUnit unit,
        KeyedCallableSupplier<K, V>... tasks) {
        return supplyKeyedCallableAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public static <K, V> Map<K, V> supplyKeyedCallableAsync(long timeout, TimeUnit unit,
        Queue<KeyedCallableSupplier<K, V>> tasks) {
        return supplyKeyedCallableAsync(timeout, unit, pollAllTask(tasks));
    }

    public static <K, V> Map<K, V> supplyKeyedCallableAsync(long timeout, TimeUnit unit,
        Collection<KeyedCallableSupplier<K, V>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new HashMap<>();
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedCallableSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        Map<K, V> result = executeKeyedAsync(map, timeout, unit);
        for (KeyedCallableSupplier<K, V> task : tasks) {
            K key = task.key();
            V response = result.get(key);
            doCallback(task, response);
        }
        return result;
    }

    @SafeVarargs
    public static <T> List<T> executeAsync(long timeout, TimeUnit unit, CompletableFuture<T>... tasks) {
        return executeAsync(timeout, unit, UtilAll.newArrayList(tasks));
    }

    public static <T> List<T> executeAsync(long timeout, TimeUnit unit, Queue<CompletableFuture<T>> tasks) {
        return executeAsync(timeout, unit, pollAllTask(tasks));
    }

    public static <T> List<T> executeAsync(long timeout, TimeUnit unit, Collection<CompletableFuture<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new ArrayList<>();
        }
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getResultIgnoreException(tasks, timeout, unit);
    }

    public static <T> List<T> getResultIgnoreException(Collection<CompletableFuture<T>> tasks, long timeout,
        TimeUnit unit) {
        List<T> result = new ArrayList<>(tasks.size());
        for (CompletableFuture<T> completableFuture : tasks) {
            if (null == completableFuture) {
                continue;
            }
            try {
                T response = completableFuture.get(timeout, unit);
                if (null != response) {
                    result.add(response);
                }
            } catch (Exception e) {
                log.error("task:{} execute failed with exception:{}", completableFuture, e.getMessage(), e);
            }
        }
        return result;
    }

    @SafeVarargs
    public static <K, V> Map<K, V> supplyKeyedAsync(KeyedSupplier<K, V>... tasks) {
        return supplyKeyedAsync(UtilAll.newArrayList(tasks));
    }

    public static <K, V> Map<K, V> supplyKeyedAsync(Queue<KeyedSupplier<K, V>> tasks) {
        return supplyKeyedAsync(pollAllTask(tasks));
    }

    public static <K, V> Map<K, V> supplyKeyedAsync(Collection<KeyedSupplier<K, V>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new HashMap<>(0);
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeKeyedAsync(map);
    }

    public static <K, V> Map<K, V> executeKeyedAsync(Map<K, CompletableFuture<V>> tasks) {
        if (MapUtils.isEmpty(tasks)) {
            return new HashMap<>(0);
        }
        try {
            CompletableFuture.allOf(tasks.values().toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getKeyedResultIgnoreException(tasks);
    }

    public static <K, V> Map<K, V> getKeyedResultIgnoreException(Map<K, CompletableFuture<V>> tasks) {
        Map<K, V> result = new HashMap<>(tasks.size());
        for (Map.Entry<K, CompletableFuture<V>> entry : tasks.entrySet()) {
            K key = entry.getKey();
            CompletableFuture<V> value = entry.getValue();
            if (null == value) {
                continue;
            }
            try {
                V response = value.get();
                if (null != response) {
                    result.put(key, response);
                }
            } catch (Exception e) {
                log.error("task with key:{} execute failed with exception:{}", key, e.getMessage(), e);
            }
        }
        return result;
    }

    @SafeVarargs
    public static <K, V> Map<K, V> supplyKeyedAsync(long timeout, TimeUnit unit, KeyedSupplier<K, V>... tasks) {
        return supplyKeyedAsync(UtilAll.newArrayList(tasks), timeout, unit);
    }

    public static <K, V> Map<K, V> supplyKeyedAsync(long timeout, TimeUnit unit, Queue<KeyedSupplier<K, V>> tasks) {
        return supplyKeyedAsync(pollAllTask(tasks), timeout, unit);
    }

    public static <K, V> Map<K, V> supplyKeyedAsync(Collection<KeyedSupplier<K, V>> tasks, long timeout,
        TimeUnit unit) {
        if (CollectionUtils.isEmpty(tasks)) {
            return new HashMap<>(0);
        }
        Map<K, CompletableFuture<V>> map = new HashMap<>(tasks.size());
        for (KeyedSupplier<K, V> task : tasks) {
            map.put(task.key(), CompletableFuture.supplyAsync(task, enginePool));
        }
        return executeKeyedAsync(map, timeout, unit);
    }

    public static <K, V> Map<K, V> executeKeyedAsync(Map<K, CompletableFuture<V>> tasks, long timeout, TimeUnit unit) {
        if (MapUtils.isEmpty(tasks)) {
            return new HashMap<>(0);
        }
        try {
            CompletableFuture.allOf(tasks.values().toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("tasks executeAsync failed with exception:{}", e.getMessage(), e);
            e.printStackTrace();
        }
        return getKeyedResultIgnoreException(tasks, timeout, unit);
    }

    public static <K, V> Map<K, V> getKeyedResultIgnoreException(Map<K, CompletableFuture<V>> tasks, long timeout,
        TimeUnit unit) {
        Map<K, V> result = new HashMap<>(tasks.size());
        for (Map.Entry<K, CompletableFuture<V>> entry : tasks.entrySet()) {
            K key = entry.getKey();
            CompletableFuture<V> value = entry.getValue();
            if (null == value) {
                continue;
            }
            try {
                V response = value.get(timeout, unit);
                if (null != response) {
                    result.put(key, response);
                }
            } catch (Exception e) {
                log.error("task with key:{} execute failed with exception:{}", key, e.getMessage(), e);
            }
        }
        return result;
    }

    public static void shutdown() {
        if (null == enginePool) {
            return;
        }
        if (!enginePool.isShutdown()) {
            enginePool.shutdown();
        }
    }
}
