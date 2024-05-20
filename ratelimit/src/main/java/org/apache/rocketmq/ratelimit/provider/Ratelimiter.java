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
package org.apache.rocketmq.ratelimit.provider;

import static com.google.common.base.Preconditions.checkArgument;

public class Ratelimiter {
    private long excess;
    private long last = System.currentTimeMillis();

    public Ratelimiter() {
    }

    public boolean canAcquire(Config config, long now, int permits) {
        synchronized (this) {
            long elapsed = now - last;
            return excess - config.rate * elapsed + permits * 1000L <= config.burst;
        }
    }

    public final void reserve(Config config, long now, int permits) {
        checkPermits(permits);
        synchronized (this) {
            long elapsed = now - last;
            excess = Math.max(excess - (long) (config.rate * elapsed) + permits * 1000L, 0);
            last = now;
        }
    }

    private static void checkPermits(int permits) {
        checkArgument(permits > 0, "Requested permits (%s) must be positive", permits);
    }

    public static class Config {
        private final double rate;
        private final long burst;

        public Config(double rate, double burst) {
            checkArgument(rate > 0 && burst > 0, "rate must be positive");
            this.rate = rate;
            this.burst = (long) (burst * 1000);
        }
    }

    @Override
    public String toString() {
        return "RateLimiter{" +
                "excess=" + excess +
                ", last=" + last +
                '}';
    }
}
