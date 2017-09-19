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

package org.apache.rocketmq.rpc.impl.metrics;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class ThreadStats {
    private final ConcurrentHashMap<Threading, TimestampRegion> statsTable = new ConcurrentHashMap<Threading, TimestampRegion>(64);

    public void beginInvoke(final long beginTimestamp) {
        Threading th = new Threading(Thread.currentThread().getName(), Thread.currentThread().getId());

        TimestampRegion tr = this.statsTable.get(th);
        if (null == tr) {
            tr = new TimestampRegion();
            this.statsTable.put(th, tr);
        }

        tr.setBeginTimestamp(beginTimestamp);
        tr.setEndTimestamp(-1);
    }

    public void endInvoke(final long endTimestamp) {
        Threading th = new Threading(Thread.currentThread().getName(), Thread.currentThread().getId());
        TimestampRegion tr = this.statsTable.get(th);
        tr.setEndTimestamp(endTimestamp);
    }

    public TreeMap<Threading, TimestampRegion> cloneStatsTable() {
        TreeMap<Threading, TimestampRegion> result = new TreeMap<Threading, TimestampRegion>();

        for (final Map.Entry<Threading, TimestampRegion> next : this.statsTable.entrySet()) {
            result.put(next.getKey(), next.getValue());
        }
        return result;
    }

}
