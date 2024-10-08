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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StatisticsItemScheduledIncrementPrinter extends StatisticsItemScheduledPrinter {

    private String[] tpsItemNames;

    public static final int TPS_INITIAL_DELAY = 0;
    public static final int TPS_INTERVAL = 1000;
    public static final String SEPARATOR = "|";

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> lastItemSnapshots
            = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItemSampleBrief>> sampleBriefs
            = new ConcurrentHashMap<>();

    public StatisticsItemScheduledIncrementPrinter(String name, StatisticsItemPrinter printer,
                                                   ScheduledExecutorService executor, InitialDelay initialDelay,
                                                   long interval, String[] tpsItemNames, Valve valve) {
        super(name, printer, executor, initialDelay, interval, valve);
        this.tpsItemNames = tpsItemNames;
    }

    @Override
    public void schedule(final StatisticsItem item) {
        setItemSampleBrief(item.getStatKind(), item.getStatObject(), new StatisticsItemSampleBrief(item, tpsItemNames));

        ScheduledFuture printFuture = executor.scheduleAtFixedRate(() -> {
            if (!enabled()) {
                return;
            }

            StatisticsItem snapshot = item.snapshot();
            StatisticsItem lastSnapshot = getItemSnapshot(lastItemSnapshots, item.getStatKind(), item.getStatObject());
            StatisticsItem increment = snapshot.subtract(lastSnapshot);

            Interceptor interceptor = item.getInterceptor();
            String interceptorStr = formatInterceptor(interceptor);
            if (interceptor != null) {
                interceptor.reset();
            }

            StatisticsItemSampleBrief brief = getSampleBrief(item.getStatKind(), item.getStatObject());
            if (brief != null && (!increment.allZeros() || printZeroLine())) {
                printer.print(name, increment, interceptorStr, brief.toString());
            }

            setItemSnapshot(lastItemSnapshots, snapshot);

            if (brief != null) {
                brief.reset();
            }
        }, getInitialDelay(), interval, TimeUnit.MILLISECONDS);
        addFuture(item, printFuture);

        ScheduledFuture sampleFuture = executor.scheduleAtFixedRate(() -> {
            if (!enabled()) {
                return;
            }

            StatisticsItem snapshot = item.snapshot();
            StatisticsItemSampleBrief brief = getSampleBrief(item.getStatKind(), item.getStatObject());
            if (brief != null) {
                brief.sample(snapshot);
            }
        }, TPS_INITIAL_DELAY, TPS_INTERVAL, TimeUnit.MILLISECONDS);
        addFuture(item, sampleFuture);
    }

    @Override
    public void remove(StatisticsItem item) {
        removeAllFuture(item);

        String kind = item.getStatKind();
        String key = item.getStatObject();

        lastItemSnapshots.computeIfPresent(kind, (k, map) -> {
            map.remove(key);
            return map.isEmpty() ? null : map;
        });

        sampleBriefs.computeIfPresent(kind, (k, map) -> {
            map.remove(key);
            return map.isEmpty() ? null : map;
        });
    }

    private StatisticsItem getItemSnapshot(
            ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> snapshots,
            String kind, String key) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = snapshots.get(kind);
        return (itemMap != null) ? itemMap.get(key) : null;
    }

    private StatisticsItemSampleBrief getSampleBrief(String kind, String key) {
        ConcurrentHashMap<String, StatisticsItemSampleBrief> itemMap = sampleBriefs.get(kind);
        return (itemMap != null) ? itemMap.get(key) : null;
    }

    private void setItemSnapshot(ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> snapshots,
                                 StatisticsItem item) {
        String kind = item.getStatKind();
        String key = item.getStatObject();
        ConcurrentHashMap<String, StatisticsItem> itemMap = snapshots.computeIfAbsent(kind, k -> new ConcurrentHashMap<>());
        itemMap.put(key, item);
    }

    private void setItemSampleBrief(String kind, String key,
                                    StatisticsItemSampleBrief brief) {
        ConcurrentHashMap<String, StatisticsItemSampleBrief> itemMap = sampleBriefs.computeIfAbsent(kind, k -> new ConcurrentHashMap<>());
        itemMap.put(key, brief);
    }

    private String formatInterceptor(Interceptor interceptor) {
        if (interceptor == null) {
            return "";
        }

        if (interceptor instanceof StatisticsBriefInterceptor) {
            StringBuilder sb = new StringBuilder();
            StatisticsBriefInterceptor briefInterceptor = (StatisticsBriefInterceptor) interceptor;
            for (StatisticsBrief brief : briefInterceptor.getStatisticsBriefs()) {
                long max = brief.getMax();
                long tp999 = Math.min(brief.tp999(), max);
                sb.append(SEPARATOR).append(max);
                sb.append(SEPARATOR).append(String.format("%.2f", brief.getAvg()));
                sb.append(SEPARATOR).append(tp999);
            }
            return sb.toString();
        }
        return "";
    }

    public static class StatisticsItemSampleBrief {
        private StatisticsItem lastSnapshot;
        private String[] itemNames;
        private ItemSampleBrief[] briefs;

        public StatisticsItemSampleBrief(StatisticsItem statItem, String[] itemNames) {
            this.lastSnapshot = statItem.snapshot();
            this.itemNames = itemNames;
            this.briefs = new ItemSampleBrief[itemNames.length];
            for (int i = 0; i < itemNames.length; i++) {
                this.briefs[i] = new ItemSampleBrief();
            }
        }

        public synchronized void reset() {
            for (ItemSampleBrief brief : briefs) {
                brief.reset();
            }
        }

        public synchronized void sample(StatisticsItem snapshot) {
            if (snapshot == null) {
                return;
            }

            for (int i = 0; i < itemNames.length; i++) {
                String name = itemNames[i];

                long lastValue = lastSnapshot != null ? lastSnapshot.getItemAccumulate(name).get() : 0;
                long increment = snapshot.getItemAccumulate(name).get() - lastValue;
                briefs[i].sample(increment);
            }
            lastSnapshot = snapshot;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < briefs.length; i++) {
                ItemSampleBrief brief = briefs[i];
                sb.append(SEPARATOR).append(brief.getMax());
                sb.append(SEPARATOR).append(String.format("%.2f", brief.getAvg()));
            }
            return sb.toString();
        }
    }

    public static class ItemSampleBrief {
        private long max;
        private long min;
        private long total;
        private long cnt;

        public ItemSampleBrief() {
            reset();
        }

        public void sample(long value) {
            max = Math.max(max, value);
            min = Math.min(min, value);
            total += value;
            cnt++;
        }

        public void reset() {
            max = 0;
            min = Long.MAX_VALUE;
            total = 0;
            cnt = 0;
        }

        public long getMax() {
            return max;
        }

        public long getMin() {
            return cnt > 0 ? min : 0;
        }

        public long getTotal() {
            return total;
        }

        public long getCnt() {
            return cnt;
        }

        public double getAvg() {
            return cnt != 0 ? ((double) total) / cnt : 0;
        }
    }
}
