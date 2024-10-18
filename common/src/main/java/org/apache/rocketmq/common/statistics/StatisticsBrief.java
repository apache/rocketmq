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
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StatisticsBrief {
    public static final int META_RANGE_INDEX = 0;
    public static final int META_SLOT_NUM_INDEX = 1;

    private long[][] topPercentileMeta;
    private AtomicInteger[] counts;
    private AtomicLong totalCount;

    private long max;
    private long min;
    private long total;

    public StatisticsBrief(long[][] topPercentileMeta) {
        if (!isLegalMeta(topPercentileMeta)) {
            throw new IllegalArgumentException("illegal topPercentileMeta");
        }

        this.topPercentileMeta = topPercentileMeta;
        this.counts = new AtomicInteger[slotNum(topPercentileMeta)];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = new AtomicInteger(0);
        }
        this.totalCount = new AtomicLong(0);
        reset();
    }

    public void reset() {
        for (AtomicInteger count : counts) {
            count.set(0);
        }
        totalCount.set(0);

        synchronized (this) {
            max = 0;
            min = Long.MAX_VALUE;
            total = 0;
        }
    }

    private static boolean isLegalMeta(long[][] meta) {
        if (ArrayUtils.isEmpty(meta)) {
            return false;
        }

        for (long[] line : meta) {
            if (ArrayUtils.isEmpty(line) || line.length != 2) {
                return false;
            }
        }

        return true;
    }

    private static int slotNum(long[][] meta) {
        int ret = 1;
        for (long[] line : meta) {
            ret += line[META_SLOT_NUM_INDEX];
        }
        return ret;
    }

    public void sample(long value) {
        int index = getSlotIndex(value);
        counts[index].incrementAndGet();
        totalCount.incrementAndGet();

        synchronized (this) {
            max = Math.max(max, value);
            min = Math.min(min, value);
            total += value;
        }
    }

    public long tp999() {
        return getTPValue(0.999f);
    }

    public long getTPValue(float ratio) {
        if (ratio <= 0 || ratio >= 1) {
            ratio = 0.99f;
        }
        long count = totalCount.get();
        long excludes = (long) (count - count * ratio);
        if (excludes == 0) {
            return getMax();
        }

        int tmp = 0;
        for (int i = counts.length - 1; i >= 0; i--) {
            tmp += counts[i].get();
            if (tmp > excludes) {
                return Math.min(getSlotTPValue(i), getMax());
            }
        }
        return 0;
    }

    private long getSlotTPValue(int index) {
        int slotNumLeft = index;
        for (int i = 0; i < topPercentileMeta.length; i++) {
            int slotNum = (int) topPercentileMeta[i][META_SLOT_NUM_INDEX];
            if (slotNumLeft < slotNum) {
                long metaRangeMax = topPercentileMeta[i][META_RANGE_INDEX];
                long metaRangeMin = (i > 0) ? topPercentileMeta[i - 1][META_RANGE_INDEX] : 0;

                return metaRangeMin + (metaRangeMax - metaRangeMin) / slotNum * (slotNumLeft + 1);
            } else {
                slotNumLeft -= slotNum;
            }
        }
        return Long.MAX_VALUE; // The last slot
    }

    private int getSlotIndex(long num) {
        int index = 0;
        for (int i = 0; i < topPercentileMeta.length; i++) {
            long rangeMax = topPercentileMeta[i][META_RANGE_INDEX];
            int slotNum = (int) topPercentileMeta[i][META_SLOT_NUM_INDEX];
            long rangeMin = (i > 0) ? topPercentileMeta[i - 1][META_RANGE_INDEX] : 0;
            if (rangeMin <= num && num < rangeMax) {
                index += (num - rangeMin) / ((rangeMax - rangeMin) / slotNum);
                break;
            }

            index += slotNum;
        }
        return index;
    }

    public long getMax() {
        return max;
    }

    public long getMin() {
        return totalCount.get() > 0 ? min : 0;
    }

    public long getTotal() {
        return total;
    }

    public long getCnt() {
        return totalCount.get();
    }

    public double getAvg() {
        return totalCount.get() != 0 ? ((double) total) / totalCount.get() : 0;
    }
}
