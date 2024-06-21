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
package org.apache.rocketmq.test.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Generated;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static java.math.BigDecimal.ROUND_HALF_UP;

@Generated("StatUtil")
public class StatUtil {
    private static Logger sysLogger = LoggerFactory.getLogger(StatUtil.class);
    private static Logger logger = LoggerFactory.getLogger("StatLogger");
    private static final int MAX_KEY_NUM = Integer.parseInt(System.getProperty("stat.util.key.max.num", "10000"));
    private static volatile ConcurrentMap<String, Invoke> invokeCache = new ConcurrentHashMap<>(64);
    private static volatile ConcurrentMap<String, Map<Long, SecondInvoke>> secondInvokeCache = new ConcurrentHashMap<>(
        64);

    private static final int STAT_WINDOW_SECONDS = Integer.parseInt(System.getProperty("stat.win.seconds", "60"));
    private static final String SPLITTER = "|";
    private static ScheduledExecutorService daemon = Executors.newSingleThreadScheduledExecutor();

    static class Invoke {
        AtomicLong totalPv = new AtomicLong();
        AtomicLong failPv = new AtomicLong();
        AtomicLong sumRt = new AtomicLong();
        AtomicLong maxRt = new AtomicLong();
        AtomicLong minRt = new AtomicLong();
        AtomicInteger topSecondPv = new AtomicInteger();
        AtomicInteger secondPv = new AtomicInteger();
        AtomicLong second = new AtomicLong(System.currentTimeMillis() / 1000L);
    }

    static class SecondInvoke implements Comparable<SecondInvoke> {
        AtomicLong total = new AtomicLong();
        AtomicLong fail = new AtomicLong();
        AtomicLong sumRt = new AtomicLong();
        AtomicLong maxRt = new AtomicLong();
        AtomicLong minRt = new AtomicLong();
        Long second = nowSecond();

        @Override
        public int compareTo(SecondInvoke o) {
            return o.second.compareTo(second);
        }
    }

    static {
        daemon.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printInvokeStat();
                    printSecondInvokeStat();
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }, STAT_WINDOW_SECONDS, STAT_WINDOW_SECONDS, TimeUnit.SECONDS);
    }

    private static void printInvokeStat() {
        Map<String, Invoke> tmp = invokeCache;
        invokeCache = new ConcurrentHashMap<>(64);

        sysLogger.warn("printInvokeStat key count:{}", tmp.size());
        for (Map.Entry<String, Invoke> entry : tmp.entrySet()) {
            String key = entry.getKey();
            Invoke invoke = entry.getValue();
            logger.warn("{}",
                buildLog(key, invoke.topSecondPv.get(), invoke.totalPv.get(), invoke.failPv.get(), invoke.minRt.get(),
                    invoke.maxRt.get(), invoke.sumRt.get()));
        }
    }

    private static void printSecondInvokeStat() {
        sysLogger.warn("printSecondInvokeStat key count:{}", secondInvokeCache.size());
        for (Map.Entry<String, Map<Long, SecondInvoke>> entry : secondInvokeCache.entrySet()) {
            String key = entry.getKey();
            Map<Long, SecondInvoke> secondInvokeMap = entry.getValue();
            long totalPv = 0L;
            long failPv = 0L;
            long topSecondPv = 0L;
            long sumRt = 0L;
            long maxRt = 0L;
            long minRt = 0L;

            for (Map.Entry<Long, SecondInvoke> invokeEntry : secondInvokeMap.entrySet()) {
                long second = invokeEntry.getKey();
                SecondInvoke secondInvoke = invokeEntry.getValue();
                if (nowSecond() - second >= STAT_WINDOW_SECONDS) {
                    secondInvokeMap.remove(second);
                    continue;
                }
                long secondPv = secondInvoke.total.get();
                totalPv += secondPv;
                failPv += secondInvoke.fail.get();
                sumRt += secondInvoke.sumRt.get();
                if (maxRt < secondInvoke.maxRt.get()) {
                    maxRt = secondInvoke.maxRt.get();
                }
                if (minRt > secondInvoke.minRt.get()) {
                    minRt = secondInvoke.minRt.get();
                }
                if (topSecondPv < secondPv) {
                    topSecondPv = secondPv;
                }
            }
            if (secondInvokeMap.isEmpty()) {
                secondInvokeCache.remove(key);
                continue;
            }
            logger.warn("{}", buildLog(key, topSecondPv, totalPv, failPv, minRt, maxRt, sumRt));
        }
    }

    private static String buildLog(String key, long topSecondPv, long totalPv, long failPv, long minRt, long maxRt,
        long sumRt) {
        StringBuilder sb = new StringBuilder();
        sb.append(SPLITTER);
        sb.append(key);
        sb.append(SPLITTER);
        sb.append(topSecondPv);
        sb.append(SPLITTER);
        int tps = new BigDecimal(totalPv).divide(new BigDecimal(STAT_WINDOW_SECONDS),
            ROUND_HALF_UP).intValue();
        sb.append(tps);
        sb.append(SPLITTER);
        sb.append(totalPv);
        sb.append(SPLITTER);
        sb.append(failPv);
        sb.append(SPLITTER);
        sb.append(minRt);
        sb.append(SPLITTER);
        long avg = new BigDecimal(sumRt).divide(new BigDecimal(totalPv),
            ROUND_HALF_UP).longValue();
        sb.append(avg);
        sb.append(SPLITTER);
        sb.append(maxRt);
        return sb.toString();
    }

    public static String buildKey(String... keys) {
        if (keys == null || keys.length <= 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String key : keys) {
            sb.append(key);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static void addInvoke(String key, long rt) {
        addInvoke(key, rt, true);
    }

    private static Invoke getAndSetInvoke(String key) {
        Invoke invoke = invokeCache.get(key);
        if (invoke == null) {
            invokeCache.putIfAbsent(key, new Invoke());
        }
        return invokeCache.get(key);
    }

    public static void addInvoke(String key, int num, long rt, boolean success) {
        if (invokeCache.size() > MAX_KEY_NUM || secondInvokeCache.size() > MAX_KEY_NUM) {
            return;
        }
        Invoke invoke = getAndSetInvoke(key);
        if (invoke == null) {
            return;
        }

        invoke.totalPv.getAndAdd(num);
        if (!success) {
            invoke.failPv.getAndAdd(num);
        }
        long now = nowSecond();
        AtomicLong oldSecond = invoke.second;
        if (oldSecond.get() == now) {
            invoke.secondPv.getAndAdd(num);
        } else {
            if (oldSecond.compareAndSet(oldSecond.get(), now)) {
                if (invoke.secondPv.get() > invoke.topSecondPv.get()) {
                    invoke.topSecondPv.set(invoke.secondPv.get());
                }
                invoke.secondPv.set(num);
            } else {
                invoke.secondPv.getAndAdd(num);
            }
        }

        invoke.sumRt.addAndGet(rt);
        if (invoke.maxRt.get() < rt) {
            invoke.maxRt.set(rt);
        }
        if (invoke.minRt.get() > rt) {
            invoke.minRt.set(rt);
        }
    }

    public static void addInvoke(String key, long rt, boolean success) {
        if (invokeCache.size() > MAX_KEY_NUM || secondInvokeCache.size() > MAX_KEY_NUM) {
            return;
        }
        Invoke invoke = getAndSetInvoke(key);
        if (invoke == null) {
            return;
        }

        invoke.totalPv.getAndIncrement();
        if (!success) {
            invoke.failPv.getAndIncrement();
        }
        long now = nowSecond();
        AtomicLong oldSecond = invoke.second;
        if (oldSecond.get() == now) {
            invoke.secondPv.getAndIncrement();
        } else {
            if (oldSecond.compareAndSet(oldSecond.get(), now)) {
                if (invoke.secondPv.get() > invoke.topSecondPv.get()) {
                    invoke.topSecondPv.set(invoke.secondPv.get());
                }
                invoke.secondPv.set(1);
            } else {
                invoke.secondPv.getAndIncrement();
            }
        }

        invoke.sumRt.addAndGet(rt);
        if (invoke.maxRt.get() < rt) {
            invoke.maxRt.set(rt);
        }
        if (invoke.minRt.get() > rt) {
            invoke.minRt.set(rt);
        }
    }

    public static SecondInvoke getAndSetSecondInvoke(String key) {
        if (!secondInvokeCache.containsKey(key)) {
            secondInvokeCache.putIfAbsent(key, new ConcurrentHashMap<>(STAT_WINDOW_SECONDS));
        }
        Map<Long, SecondInvoke> secondInvokeMap = secondInvokeCache.get(key);
        if (secondInvokeMap == null) {
            return null;
        }
        long second = nowSecond();
        if (!secondInvokeMap.containsKey(second)) {
            secondInvokeMap.putIfAbsent(second, new SecondInvoke());
        }
        return secondInvokeMap.get(second);
    }

    public static void addSecondInvoke(String key, long rt) {
        addSecondInvoke(key, rt, true);
    }

    public static void addSecondInvoke(String key, long rt, boolean success) {
        if (invokeCache.size() > MAX_KEY_NUM || secondInvokeCache.size() > MAX_KEY_NUM) {
            return;
        }
        SecondInvoke secondInvoke = getAndSetSecondInvoke(key);
        if (secondInvoke == null) {
            return;
        }
        secondInvoke.total.addAndGet(1);
        if (!success) {
            secondInvoke.fail.addAndGet(1);
        }
        secondInvoke.sumRt.addAndGet(rt);
        if (secondInvoke.maxRt.get() < rt) {
            secondInvoke.maxRt.set(rt);
        }
        if (secondInvoke.minRt.get() > rt) {
            secondInvoke.minRt.set(rt);
        }
    }

    public static void addPv(String key, long totalPv) {
        addPv(key, totalPv, true);
    }

    public static void addPv(String key, long totalPv, boolean success) {
        if (invokeCache.size() > MAX_KEY_NUM || secondInvokeCache.size() > MAX_KEY_NUM) {
            return;
        }
        if (totalPv <= 0) {
            return;
        }
        Invoke invoke = getAndSetInvoke(key);
        if (invoke == null) {
            return;
        }
        invoke.totalPv.addAndGet(totalPv);
        if (!success) {
            invoke.failPv.addAndGet(totalPv);
        }
        long now = nowSecond();
        AtomicLong oldSecond = invoke.second;
        if (oldSecond.get() == now) {
            invoke.secondPv.addAndGet((int)totalPv);
        } else {
            if (oldSecond.compareAndSet(oldSecond.get(), now)) {
                if (invoke.secondPv.get() > invoke.topSecondPv.get()) {
                    invoke.topSecondPv.set(invoke.secondPv.get());
                }
                invoke.secondPv.set((int)totalPv);
            } else {
                invoke.secondPv.addAndGet((int)totalPv);
            }
        }
    }

    public static void addSecondPv(String key, long totalPv) {
        addSecondPv(key, totalPv, true);
    }

    public static void addSecondPv(String key, long totalPv, boolean success) {
        if (invokeCache.size() > MAX_KEY_NUM || secondInvokeCache.size() > MAX_KEY_NUM) {
            return;
        }
        if (totalPv <= 0) {
            return;
        }
        SecondInvoke secondInvoke = getAndSetSecondInvoke(key);
        if (secondInvoke == null) {
            return;
        }
        secondInvoke.total.addAndGet(totalPv);
        if (!success) {
            secondInvoke.fail.addAndGet(totalPv);
        }
    }

    public static boolean isOverFlow(String key, int tps) {
        return nowTps(key) >= tps;
    }

    public static int nowTps(String key) {
        Map<Long, SecondInvoke> secondInvokeMap = secondInvokeCache.get(key);
        if (secondInvokeMap != null) {
            SecondInvoke secondInvoke = secondInvokeMap.get(nowSecond());
            if (secondInvoke != null) {
                return (int)secondInvoke.total.get();
            }
        }
        Invoke invoke = invokeCache.get(key);
        if (invoke == null) {
            return 0;
        }
        AtomicLong oldSecond = invoke.second;
        if (oldSecond.get() == nowSecond()) {
            return invoke.secondPv.get();
        }
        return 0;
    }

    public static int totalPvInWindow(String key, int windowSeconds) {
        List<SecondInvoke> list = secondInvokeList(key, windowSeconds);
        long totalPv = 0;
        for (int i = 0; i < windowSeconds && i < list.size(); i++) {
            totalPv += list.get(i).total.get();
        }
        return (int)totalPv;
    }

    public static int failPvInWindow(String key, int windowSeconds) {
        List<SecondInvoke> list = secondInvokeList(key, windowSeconds);
        long failPv = 0;
        for (int i = 0; i < windowSeconds && i < list.size(); i++) {
            failPv += list.get(i).fail.get();
        }
        return (int)failPv;
    }

    public static int topTpsInWindow(String key, int windowSeconds) {
        List<SecondInvoke> list = secondInvokeList(key, windowSeconds);
        long topTps = 0;
        for (int i = 0; i < windowSeconds && i < list.size(); i++) {
            long secondPv = list.get(i).total.get();
            if (topTps < secondPv) {
                topTps = secondPv;
            }
        }
        return (int)topTps;
    }

    public static int avgRtInWindow(String key, int windowSeconds) {
        List<SecondInvoke> list = secondInvokeList(key, windowSeconds);
        long sumRt = 0;
        long totalPv = 0;
        for (int i = 0; i < windowSeconds && i < list.size(); i++) {
            sumRt += list.get(i).sumRt.get();
            totalPv += list.get(i).total.get();
        }
        if (totalPv <= 0) {
            return 0;
        }
        long avg = new BigDecimal(sumRt).divide(new BigDecimal(totalPv),
            ROUND_HALF_UP).longValue();
        return (int)avg;
    }

    public static int maxRtInWindow(String key, int windowSeconds) {
        List<SecondInvoke> list = secondInvokeList(key, windowSeconds);
        long maxRt = 0;
        long totalPv = 0;
        for (int i = 0; i < windowSeconds && i < list.size(); i++) {
            if (maxRt < list.get(i).maxRt.get()) {
                maxRt = list.get(i).maxRt.get();
            }
        }
        return (int)maxRt;
    }

    public static int minRtInWindow(String key, int windowSeconds) {
        List<SecondInvoke> list = secondInvokeList(key, windowSeconds);
        long minRt = 0;
        long totalPv = 0;
        for (int i = 0; i < windowSeconds && i < list.size(); i++) {
            if (minRt < list.get(i).minRt.get()) {
                minRt = list.get(i).minRt.get();
            }
        }
        return (int)minRt;
    }

    private static List<SecondInvoke> secondInvokeList(String key, int windowSeconds) {
        if (windowSeconds > STAT_WINDOW_SECONDS || windowSeconds <= 0) {
            throw new IllegalArgumentException("windowSeconds Must Not be great than " + STAT_WINDOW_SECONDS);
        }
        Map<Long, SecondInvoke> secondInvokeMap = secondInvokeCache.get(key);
        if (secondInvokeMap == null || secondInvokeMap.isEmpty()) {
            return new ArrayList<>();
        }
        List<SecondInvoke> list = new ArrayList<>();
        list.addAll(secondInvokeMap.values());
        Collections.sort(list);
        return list;
    }

    private static long nowSecond() {
        return System.currentTimeMillis() / 1000L;
    }

}
