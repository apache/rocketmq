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
package org.apache.rocketmq.remoting.netty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe tracker for per-requestCode count and traffic distribution.
 * <p>
 */
public class RemotingCodeDistributionHandler {

    private final ConcurrentMap<Integer, TrafficStats> inboundStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, TrafficStats> outboundStats = new ConcurrentHashMap<>();

    public void recordInbound(int code, int wireSize) {
        TrafficStats stats = inboundStats.computeIfAbsent(code, k -> new TrafficStats());
        stats.count.increment();
        stats.trafficSize.add(wireSize);
    }

    public void recordOutbound(int code, int wireSize) {
        TrafficStats stats = outboundStats.computeIfAbsent(code, k -> new TrafficStats());
        stats.count.increment();
        stats.trafficSize.add(wireSize);
    }

    public String getInBoundSnapshotString() {
        return snapshotToString(getSnapshot(inboundStats, true));
    }

    public String getOutBoundSnapshotString() {
        return snapshotToString(getSnapshot(outboundStats, true));
    }

    public String getInBoundTrafficSnapshotString() {
        return snapshotToString(getSnapshot(inboundStats, false));
    }

    public String getOutBoundTrafficSnapshotString() {
        return snapshotToString(getSnapshot(outboundStats, false));
    }

    private Map<Integer, Long> getSnapshot(ConcurrentMap<Integer, TrafficStats> statsMap, boolean count) {
        Map<Integer, Long> map = new HashMap<>(statsMap.size());
        for (Map.Entry<Integer, TrafficStats> entry : statsMap.entrySet()) {
            LongAdder adder = count ? entry.getValue().count : entry.getValue().trafficSize;
            map.put(entry.getKey(), adder.sumThenReset());
        }
        return map;
    }

    private String snapshotToString(Map<Integer, Long> distribution) {
        if (null == distribution || distribution.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Integer, Long> entry : distribution.entrySet()) {
            if (0L == entry.getValue()) {
                continue;
            }
            sb.append(first ? "" : ", ").append(entry.getKey()).append(":").append(entry.getValue());
            first = false;
        }
        return first ? null : sb.append("}").toString();
    }

    static class TrafficStats {
        final LongAdder count = new LongAdder();
        final LongAdder trafficSize = new LongAdder();
    }
}
