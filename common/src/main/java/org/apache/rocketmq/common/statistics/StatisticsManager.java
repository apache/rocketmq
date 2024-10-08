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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.utils.ThreadUtils;

public class StatisticsManager {

    private final Map<String, StatisticsKindMeta> kindMetaMap = new HashMap<>();
    private Pair<String, long[][]>[] briefMetas;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticsItem>> statsTable
            = new ConcurrentHashMap<>();

    private static final int MAX_IDLE_TIME = 10 * 60 * 1000;
    private final ScheduledExecutorService executor = ThreadUtils.newSingleThreadScheduledExecutor(
            "StatisticsManagerCleaner", true);

    private StatisticsItemStateGetter statisticsItemStateGetter;

    public StatisticsManager() {
        start();
    }

    public StatisticsManager(Map<String, StatisticsKindMeta> kindMeta) {
        this.kindMetaMap.putAll(kindMeta);
        start();
    }

    public void addStatisticsKindMeta(StatisticsKindMeta kindMeta) {
        kindMetaMap.put(kindMeta.getName(), kindMeta);
        statsTable.putIfAbsent(kindMeta.getName(), new ConcurrentHashMap<>(16));
    }

    public void setBriefMeta(Pair<String, long[][]>[] briefMetas) {
        this.briefMetas = briefMetas;
    }

    private void start() {
        int maxIdleTime = MAX_IDLE_TIME;
        executor.scheduleAtFixedRate(() -> {
            Iterator<Map.Entry<String, ConcurrentHashMap<String, StatisticsItem>>> iter
                    = statsTable.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, ConcurrentHashMap<String, StatisticsItem>> entry = iter.next();
                String kind = entry.getKey();
                ConcurrentHashMap<String, StatisticsItem> itemMap = entry.getValue();

                if (itemMap == null || itemMap.isEmpty()) {
                    continue;
                }

                for (StatisticsItem item : itemMap.values()) {
                    // remove when expired
                    if (System.currentTimeMillis() - item.getLastTimeStamp().get() > MAX_IDLE_TIME
                            && (statisticsItemStateGetter == null || !statisticsItemStateGetter.online(item))) {
                        remove(item);
                    }
                }
            }
        }, maxIdleTime, maxIdleTime / 3, TimeUnit.MILLISECONDS);
    }

    public boolean inc(String kind, String key, long... itemAccumulates) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = statsTable.get(kind);
        if (itemMap != null) {
            StatisticsItem item = itemMap.computeIfAbsent(key, k -> {
                StatisticsItem newItem = new StatisticsItem(kind, key, kindMetaMap.get(kind).getItemNames());
                newItem.setInterceptor(new StatisticsBriefInterceptor(newItem, briefMetas));
                scheduleStatisticsItem(newItem);
                return newItem;
            });

            item.incItems(itemAccumulates);
            return true;
        }

        return false;
    }

    private void scheduleStatisticsItem(StatisticsItem item) {
        StatisticsKindMeta kindMeta = kindMetaMap.get(item.getStatKind());
        if (kindMeta != null) {
            kindMeta.getScheduledPrinter().schedule(item);
        }
    }

    public void remove(StatisticsItem item) {
        ConcurrentHashMap<String, StatisticsItem> itemMap = statsTable.get(item.getStatKind());
        if (itemMap != null) {
            itemMap.remove(item.getStatObject(), item);

            StatisticsKindMeta kindMeta = kindMetaMap.get(item.getStatKind());
            if (kindMeta != null && kindMeta.getScheduledPrinter() != null) {
                kindMeta.getScheduledPrinter().remove(item);
            }
        }
    }

    public StatisticsItemStateGetter getStatisticsItemStateGetter() {
        return statisticsItemStateGetter;
    }

    public void setStatisticsItemStateGetter(StatisticsItemStateGetter statisticsItemStateGetter) {
        this.statisticsItemStateGetter = statisticsItemStateGetter;
    }
}
