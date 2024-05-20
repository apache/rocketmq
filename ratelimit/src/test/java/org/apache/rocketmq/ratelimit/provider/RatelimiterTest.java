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

import org.junit.Assert;
import org.junit.Test;


public class RatelimiterTest {
    @Test
    public void testRateLimiter() {
        Ratelimiter.Config config = new Ratelimiter.Config(3, 3);
        Ratelimiter rateLimiter = new Ratelimiter();
        long now = System.currentTimeMillis();
        rateLimiter.reserve(config, now, 1);
        rateLimiter.reserve(config, now, 1);
        Assert.assertTrue(rateLimiter.canAcquire(config, now, 1));
        rateLimiter.reserve(config, now, 1);
        Assert.assertFalse(rateLimiter.canAcquire(config, now, 1));
        now += 1000;
        rateLimiter.reserve(config, now, 1);
        rateLimiter.reserve(config, now, 1);
        Assert.assertTrue(rateLimiter.canAcquire(config, now, 1));
        rateLimiter.reserve(config, now, 1);
        Assert.assertFalse(rateLimiter.canAcquire(config, now, 1));
    }

    @Test
    public void testRateLimiter_lowRate() {
        Ratelimiter.Config config = new Ratelimiter.Config(0.1, 0.1);
        Ratelimiter rateLimiter = new Ratelimiter();
        long start = System.currentTimeMillis();
        long now = start;
        int time = 0;
        while (true) {
            now += 46; // random sleep time
            if (rateLimiter.canAcquire(config, now, 1)) {
                rateLimiter.reserve(config, now, 1);
                time++;
                if (time >= 10) {
                    break;
                }
            }
        }
        Assert.assertTrue(now - start < 100000);
        Assert.assertTrue(now - start > 99000);
    }
}