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

package org.apache.rocketmq.common.stats;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;

public class MomentStatsItemSet {
    private final ConcurrentMap<String/* key */, MomentStatsItem> statsItemTable =
        new ConcurrentHashMap<>(128);
    private final String statsName;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Logger log;

    public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }

    public ConcurrentMap<String, MomentStatsItem> getStatsItemTable() {
        return statsItemTable;
    }

    public String getStatsName() {
        return statsName;
    }

    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    private void printAtMinutes() {
        Iterator<Entry<String, MomentStatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MomentStatsItem> next = it.next();
            next.getValue().printAtMinutes();
        }
    }

    public void setValue(final String statsKey, final int value) {
        MomentStatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().set(value);
    }

    public void delValueByInfixKey(final String statsKey, String separator) {
        Iterator<Entry<String, MomentStatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MomentStatsItem> next = it.next();
            if (next.getKey().contains(separator + statsKey + separator)) {
                it.remove();
            }
        }
    }

    public void delValueBySuffixKey(final String statsKey, String separator) {
        Iterator<Entry<String, MomentStatsItem>> it = this.statsItemTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MomentStatsItem> next = it.next();
            if (next.getKey().endsWith(separator + statsKey)) {
                it.remove();
            }
        }
    }

    public MomentStatsItem getAndCreateStatsItem(final String statsKey) {
        MomentStatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem =
                new MomentStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            MomentStatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }
}
