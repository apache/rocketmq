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
package org.apache.rocketmq.store.util;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PerfCounter {

    private long last = System.currentTimeMillis();
    private float lastTps = 0.0f;

    private final ThreadLocal<AtomicLong> lastTickMs = new ThreadLocal<AtomicLong>() {
        @Override
        protected AtomicLong initialValue() {
            return new AtomicLong(System.currentTimeMillis());
        }
    };

    private final Logger logger;
    private String prefix = "DEFAULT";

    public float getLastTps() {
        if (System.currentTimeMillis() - last <= maxTimeMsPerCount  + 3000) {
            return lastTps;
        }
        return 0.0f;
    }

    //1000 * ms, 1000 * 10 ms, then 100ms every slots
    private final AtomicInteger[] count;
    private final AtomicLong allCount;
    private final int maxNumPerCount;
    private final int maxTimeMsPerCount;


    public PerfCounter() {
        this(5001, null, null, 1000 * 1000, 10 * 1000);
    }

    public PerfCounter(int slots, Logger logger, String prefix, int maxNumPerCount, int maxTimeMsPerCount) {
        if (slots < 3000) {
            throw new RuntimeException("slots must bigger than 3000, but:%s" + slots);
        }
        count = new AtomicInteger[slots];
        allCount = new AtomicLong(0);
        this.logger = logger;
        if (prefix != null) {
            this.prefix = prefix;
        }
        this.maxNumPerCount = maxNumPerCount;
        this.maxTimeMsPerCount = maxTimeMsPerCount;
        reset();
    }

    public void flow(long cost) {
        flow(cost, 1);
    }

    public void flow(long cost, int num) {
        if (cost < 0) return;
        allCount.addAndGet(num);
        count[getIndex(cost)].addAndGet(num);
        if (allCount.get() >= maxNumPerCount
            || System.currentTimeMillis() - last >= maxTimeMsPerCount) {
            synchronized (allCount) {
                if (allCount.get() < maxNumPerCount
                    && System.currentTimeMillis() - last < maxTimeMsPerCount) {
                    return;
                }
                print();
                this.reset();
            }
        }
    }

    public void print() {
        int min = this.getMin();
        int max = this.getMax();
        int tp50 = this.getTPValue(0.5f);
        int tp80 = this.getTPValue(0.8f);
        int tp90 = this.getTPValue(0.9f);
        int tp99 = this.getTPValue(0.99f);
        int tp999 = this.getTPValue(0.999f);
        long count0t1 = this.getCount(0, 1);
        long count2t5 = this.getCount(2, 5);
        long count6t10 = this.getCount(6, 10);
        long count11t50 = this.getCount(11, 50);
        long count51t100 = this.getCount(51, 100);
        long count101t500 = this.getCount(101, 500);
        long count501t999 = this.getCount(501, 999);
        long count1000t = this.getCount(1000, 100000000);
        long elapsed = System.currentTimeMillis() - last;
        lastTps = (allCount.get() + 0.1f) * 1000 / elapsed;
        String str = String.format("PERF_COUNTER_%s[%s] num:%d cost:%d tps:%.4f min:%d max:%d tp50:%d tp80:%d tp90:%d tp99:%d tp999:%d " +
                "0_1:%d 2_5:%d 6_10:%d 11_50:%d 51_100:%d 101_500:%d 501_999:%d 1000_:%d",
            prefix, new Timestamp(System.currentTimeMillis()), allCount.get(), elapsed, lastTps,
            min, max, tp50, tp80, tp90, tp99, tp999,
            count0t1, count2t5, count6t10, count11t50, count51t100, count101t500, count501t999, count1000t);
        if (logger != null) {
            logger.info(str);
        }
    }

    private int getIndex(long cost) {
        if (cost < 1000) {
            return (int) cost;
        }
        if (cost >= 1000 && cost < 1000 + 1000 * 10) {
            int units = (int) ((cost - 1000) / 10);
            return 1000 + units;
        }
        int units = (int) ((cost - 1000 - 1000 * 10) / 100);
        units = 2000 + units;
        if (units >= count.length) {
            units = count.length - 1;
        }
        return units;
    }

    private int convert(int index) {
        if (index < 1000) {
            return index;
        } else if (index >= 1000 && index < 2000) {
            return (index - 1000) * 10 + 1000;
        } else {
            return (index - 2000) * 100 + 1000 * 10 + 1000;
        }
    }

    public float getRate(int from, int to) {
        long tmp = getCount(from, to);
        return ((tmp + 0.0f) * 100) / (allCount.get() + 1);
    }

    public long getCount(int from, int to) {
        from = getIndex(from);
        to = getIndex(to);
        long tmp = 0;
        for (int i = from; i <= to && i < count.length; i++) {
            tmp = tmp + count[i].get();
        }
        return tmp;
    }

    public int getTPValue(float ratio) {
        if (ratio <= 0 || ratio >= 1) {
            ratio = 0.99f;
        }
        long num = (long) (allCount.get() * (1 - ratio));
        int tmp = 0;
        for (int i = count.length - 1; i > 0; i--) {
            tmp += count[i].get();
            if (tmp > num) {
                return convert(i);
            }
        }
        return 0;
    }

    public int getMin() {
        for (int i = 0; i < count.length; i++) {
            if (count[i].get() > 0) {
                return convert(i);
            }
        }
        return 0;
    }

    public int getMax() {
        for (int i = count.length - 1; i > 0; i--) {
            if (count[i].get() > 0) {
                return convert(i);
            }
        }
        return 99999999;
    }

    public void reset() {
        for (int i = 0; i < count.length; i++) {
            if (count[i] == null) {
                count[i] = new AtomicInteger(0);
            } else {
                count[i].set(0);
            }
        }
        allCount.set(0);
        last = System.currentTimeMillis();
    }

    public void startTick() {
        lastTickMs.get().set(System.currentTimeMillis());
    }

    public void endTick() {
        flow(System.currentTimeMillis() - lastTickMs.get().get());
    }

    public static class Ticks extends ServiceThread {
        private final Logger logger;
        private final Map<String, PerfCounter> perfs = new ConcurrentHashMap<>();
        private final Map<String, AtomicLong> keyFreqs = new ConcurrentHashMap<>();
        private final PerfCounter defaultPerf;
        private final AtomicLong defaultTime = new AtomicLong(System.currentTimeMillis());

        private final int maxKeyNumPerf;
        private final int maxKeyNumDebug;

        private final int maxNumPerCount;
        private final int maxTimeMsPerCount;

        public Ticks() {
            this(null, 1000 * 1000, 10 * 1000, 20 * 1000, 100 * 1000);
        }

        public Ticks(Logger logger) {
            this(logger, 1000 * 1000, 10 * 1000, 20 * 1000, 100 * 1000);
        }

        @Override
        public String getServiceName() {
            return this.getClass().getName();
        }

        public Ticks(Logger logger, int maxNumPerCount, int maxTimeMsPerCount, int maxKeyNumPerf, int maxKeyNumDebug) {
            this.logger = logger;
            this.maxNumPerCount = maxNumPerCount;
            this.maxTimeMsPerCount = maxTimeMsPerCount;
            this.maxKeyNumPerf =  maxKeyNumPerf;
            this.maxKeyNumDebug =  maxKeyNumDebug;
            this.defaultPerf = new PerfCounter(3001, logger, null, maxNumPerCount, maxTimeMsPerCount);

        }

        private PerfCounter makeSureExists(String key) {
            if (perfs.get(key) == null) {
                if (perfs.size() >= maxKeyNumPerf + 100) {
                    return defaultPerf;
                }
                perfs.put(key, new PerfCounter(3001, logger, key, maxNumPerCount, maxTimeMsPerCount));
            }
            return perfs.getOrDefault(key, defaultPerf);
        }

        public void startTick(String key) {
            try {
                makeSureExists(key).startTick();
            } catch (Throwable ignored) {

            }
        }

        public void endTick(String key) {
            try {
                makeSureExists(key).endTick();
            } catch (Throwable ignored) {

            }
        }

        public void flowOnce(String key, int cost) {
            try {
                makeSureExists(key).flow(cost);
            } catch (Throwable ignored) {

            }
        }

        public PerfCounter getCounter(String key) {
            try {
                return makeSureExists(key);
            } catch (Throwable ignored) {
                return defaultPerf;
            }
        }

        private AtomicLong makeSureDebugKeyExists(String key) {
            AtomicLong lastTimeMs = keyFreqs.get(key);
            if (null == lastTimeMs) {
                if (keyFreqs.size() >= maxKeyNumDebug + 100) {
                    return defaultTime;
                }
                lastTimeMs = new AtomicLong(0);
                keyFreqs.put(key, lastTimeMs);
            }
            return keyFreqs.getOrDefault(key, defaultTime);
        }
        public boolean shouldDebugKeyAndTimeMs(String key, int intervalMs) {
            try {
                AtomicLong lastTimeMs =  makeSureDebugKeyExists(key);
                if (System.currentTimeMillis() - lastTimeMs.get() > intervalMs) {
                    lastTimeMs.set(System.currentTimeMillis());
                    return true;
                }
                return false;
            } catch (Throwable ignored) {

            }
            return false;
        }

        @Override
        public void run() {
            logger.info("{} get started", getServiceName());
            while (!this.isStopped()) {
                try {
                    long maxLiveTimeMs = maxTimeMsPerCount * 2 + 1000;
                    this.waitForRunning(maxLiveTimeMs);
                    if (perfs.size() >= maxKeyNumPerf
                            || keyFreqs.size() >= maxKeyNumDebug) {
                        logger.warn("The key is full {}-{} {}-{}", perfs.size(), maxKeyNumPerf, keyFreqs.size(), maxKeyNumDebug);
                    }
                    {
                        Iterator<Map.Entry<String, PerfCounter>> it = perfs.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<String, PerfCounter> entry = it.next();
                            PerfCounter value = entry.getValue();
                            // May have concurrency problem, but it has no effect, we can ignore it.
                            if (System.currentTimeMillis() - value.last > maxLiveTimeMs) {
                                it.remove();
                            }
                        }
                    }

                    {
                        Iterator<Map.Entry<String, AtomicLong>> it = keyFreqs.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry<String, AtomicLong> entry = it.next();
                            AtomicLong value = entry.getValue();
                            // May have concurrency problem, but it has no effect, we can ignore it.
                            if (System.currentTimeMillis() - value.get() > maxLiveTimeMs) {
                                it.remove();
                            }
                        }
                    }

                } catch (Exception e) {
                    logger.error("{} get unknown errror", getServiceName(), e);
                    try {
                        Thread.sleep(1000);
                    } catch (Throwable ignored) {

                    }
                }
            }
            logger.info("{} get stopped", getServiceName());
        }
    }
}
