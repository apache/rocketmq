/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.rocketmq.mqtt.util.orderedexecutor;

import java.util.Arrays;

/**
 * This class provides a read view of operation specific stats.
 * We expose this to JMX.
 * We use primitives because the class has to conform to CompositeViewData.
 */
public class OpStatsData {
    private final long numSuccessfulEvents, numFailedEvents;
    // All latency values are in Milliseconds.
    private final double avgLatencyMillis;
    // 10.0 50.0, 90.0, 99.0, 99.9, 99.99 in that order.
    // TODO: Figure out if we can use a Map
    private final long[] percentileLatenciesMillis;
    public OpStatsData(long numSuccessfulEvents, long numFailedEvents,
                        double avgLatencyMillis, long[] percentileLatenciesMillis) {
        this.numSuccessfulEvents = numSuccessfulEvents;
        this.numFailedEvents = numFailedEvents;
        this.avgLatencyMillis = avgLatencyMillis;
        this.percentileLatenciesMillis =
            Arrays.copyOf(percentileLatenciesMillis, percentileLatenciesMillis.length);
    }

    public long getP10Latency() {
        return this.percentileLatenciesMillis[0];
    }
    public long getP50Latency() {
        return this.percentileLatenciesMillis[1];
    }

    public long getP90Latency() {
        return this.percentileLatenciesMillis[2];
    }

    public long getP99Latency() {
        return this.percentileLatenciesMillis[3];
    }

    public long getP999Latency() {
        return this.percentileLatenciesMillis[4];
    }

    public long getP9999Latency() {
        return this.percentileLatenciesMillis[5];
    }

    public long getNumSuccessfulEvents() {
        return this.numSuccessfulEvents;
    }

    public long getNumFailedEvents() {
        return this.numFailedEvents;
    }

    public double getAvgLatencyMillis() {
        return this.avgLatencyMillis;
    }
}
