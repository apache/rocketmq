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

import java.util.TreeMap;

public class StatsAll {
    private TreeMap<String/* request code */, MethodStats> statsCaller = new TreeMap<String, MethodStats>();
    private TreeMap<String/* request code */, MethodStats> statsProvider = new TreeMap<String, MethodStats>();
    private TreeMap<Threading, TimestampRegion> statsThreading = new TreeMap<Threading, TimestampRegion>();

    public TreeMap<String, MethodStats> getStatsCaller() {
        return statsCaller;
    }

    public void setStatsCaller(TreeMap<String, MethodStats> statsCaller) {
        this.statsCaller = statsCaller;
    }

    public TreeMap<String, MethodStats> getStatsProvider() {
        return statsProvider;
    }

    public void setStatsProvider(TreeMap<String, MethodStats> statsProvider) {
        this.statsProvider = statsProvider;
    }

    public TreeMap<Threading, TimestampRegion> getStatsThreading() {
        return statsThreading;
    }

    public void setStatsThreading(TreeMap<Threading, TimestampRegion> statsThreading) {
        this.statsThreading = statsThreading;
    }

    @Override
    public String toString() {
        return "StatsAll{" +
            "statsCaller=" + statsCaller +
            ", statsProvider=" + statsProvider +
            ", statsThreading=" + statsThreading +
            '}';
    }

}
