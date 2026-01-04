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
import org.apache.commons.lang3.tuple.Pair;

/**
 * interceptor to generate statistics brief
 */
public class StatisticsBriefInterceptor implements Interceptor {
    private int[] indexOfItems;

    private StatisticsBrief[] statisticsBriefs;

    public StatisticsBriefInterceptor(StatisticsItem item, Pair<String, long[][]>[] briefMetas) {
        indexOfItems = new int[briefMetas.length];
        statisticsBriefs = new StatisticsBrief[briefMetas.length];
        for (int i = 0; i < briefMetas.length; i++) {
            String name = briefMetas[i].getKey();
            int index = ArrayUtils.indexOf(item.getItemNames(), name);
            if (index < 0) {
                throw new IllegalArgumentException("illegal briefItemName: " + name);
            }
            indexOfItems[i] = index;
            statisticsBriefs[i] = new StatisticsBrief(briefMetas[i].getValue());
        }
    }

    @Override
    public void inc(long... itemValues) {
        for (int i = 0; i < indexOfItems.length; i++) {
            int indexOfItem = indexOfItems[i];
            if (indexOfItem < itemValues.length) {
                statisticsBriefs[i].sample(itemValues[indexOfItem]);
            }
        }
    }

    @Override
    public void reset() {
        for (StatisticsBrief brief : statisticsBriefs) {
            brief.reset();
        }
    }

    public int[] getIndexOfItems() {
        return indexOfItems;
    }

    public void setIndexOfItems(int[] indexOfItems) {
        this.indexOfItems = indexOfItems;
    }

    public StatisticsBrief[] getStatisticsBriefs() {
        return statisticsBriefs;
    }

    public void setStatisticsBriefs(StatisticsBrief[] statisticsBriefs) {
        this.statisticsBriefs = statisticsBriefs;
    }
}
