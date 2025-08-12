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
package org.apache.rocketmq.remoting.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * CircuitBreakerCounter class implements a basic circuit breaker pattern with a counter mechanism.
 * It keeps track of failure counts for specific keys within a defined time window, and can prevent
 * further requests if the failure count exceeds a threshold. It also has a recovery period after
 * which it resets the counter.
 */
public class CircuitBreakerCounter {

    /**
     * Recovery time in seconds after which the counter resets
     */
    private final int recoverSecond;

    /**
     * Threshold for the number of failures within the degrade time window
     */
    private final int degradeRuleCount;

    /**
     * Time window in seconds for the degrade rule
     */
    private final int degradeRuleSecond;


    private final ConcurrentHashMap<String, CircuitBreakerTimeWindow> circuitBreakerTimeWindowMap = new ConcurrentHashMap<>();

    private final ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor(
            (Runnable runnable) -> new Thread(runnable, "CircuitBreakerCounter-clearExpireRecord"));

    /**
     * Constructs a new CircuitBreakerCounter with the specified recovery time, degrade rule count, and degrade time window.
     *
     * @param recoverSecond     The recovery time in seconds
     * @param degradeRuleCount  The threshold for the number of failures within the degrade time window
     * @param degradeRuleSecond The time window in seconds for the degrade rule
     */
    public CircuitBreakerCounter(int recoverSecond, int degradeRuleCount, int degradeRuleSecond) {
        if (recoverSecond <= 0 || degradeRuleCount <= 0 || degradeRuleSecond <= 0) {
            throw new IllegalArgumentException("degradeRuleSecond or recoverSecond or degradeRuleCount must be greater than 0");
        }

        this.recoverSecond = recoverSecond;
        this.degradeRuleCount = degradeRuleCount;
        this.degradeRuleSecond = degradeRuleSecond;
    }

    public void start() {
        schedule.scheduleAtFixedRate(this::clearExpireRecord, 10, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        circuitBreakerTimeWindowMap.clear();
        schedule.shutdown();
    }

    public void addCount(String key, int count) {
        if (schedule.isShutdown()) {
            throw new IllegalStateException("Circuit breaker has been shutdown");
        }
        final long now = System.currentTimeMillis();
        circuitBreakerTimeWindowMap.compute(key, (k, v) -> {
            final long end = now + degradeRuleSecond * 1000L;
            if (v == null) {
                v = new CircuitBreakerTimeWindow(count, end);
            } else if (v.degradeCount < degradeRuleCount) {
                v.addCount(count);
            }
            return v;
        });
    }

    public boolean check(String key) {
        CircuitBreakerTimeWindow timeWindow = circuitBreakerTimeWindowMap.get(key);
        if (timeWindow == null) {
            return true;
        }
        final long now = System.currentTimeMillis();
        final long expireTime = timeWindow.endTime + recoverSecond * 1000L;
        if (now > expireTime) {
            return true;
        }

        return timeWindow.degradeCount < degradeRuleCount;
    }

    public int getCount(String key) {
        CircuitBreakerTimeWindow timeWindow = circuitBreakerTimeWindowMap.get(key);
        return timeWindow == null ? 0 : timeWindow.degradeCount;
    }

    /**
     * Clears expired records from the map based on the recovery time.
     */
    private void clearExpireRecord() {
        circuitBreakerTimeWindowMap.entrySet().removeIf(entry -> {
            long expireTime = entry.getValue().endTime + recoverSecond * 1000L;
            return expireTime < System.currentTimeMillis();
        });
    }

    /**
     * Inner class representing a time window for the circuit breaker counter.
     */
    private static class CircuitBreakerTimeWindow {
        private int degradeCount;
        private final long endTime;

        public CircuitBreakerTimeWindow(int degradeCount, long endTime) {
            this.degradeCount = degradeCount;
            this.endTime = endTime;
        }

        void addCount(int count) {
            degradeCount += count;
        }
    }
}
