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

package org.apache.rocketmq.proxy.common;

import org.apache.rocketmq.remoting.protocol.subscription.RetryPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class RenewStrategyPolicyTest {

    private RetryPolicy retryPolicy;
    private final AtomicInteger times = new AtomicInteger(0);

    @Before
    public void before() throws Throwable {
        this.retryPolicy = new RenewStrategyPolicy();
    }

    @Test
    public void testNextDelayDuration() {
        long value = this.retryPolicy.nextDelayDuration(times.getAndIncrement());
        assertEquals(value, TimeUnit.MINUTES.toMillis(1));

        value = this.retryPolicy.nextDelayDuration(times.getAndIncrement());
        assertEquals(value, TimeUnit.MINUTES.toMillis(3));

        value = this.retryPolicy.nextDelayDuration(times.getAndIncrement());
        assertEquals(value, TimeUnit.MINUTES.toMillis(5));

        value = this.retryPolicy.nextDelayDuration(times.getAndIncrement());
        assertEquals(value, TimeUnit.MINUTES.toMillis(10));

        value = this.retryPolicy.nextDelayDuration(times.getAndIncrement());
        assertEquals(value, TimeUnit.MINUTES.toMillis(30));

        value = this.retryPolicy.nextDelayDuration(times.getAndIncrement());
        assertEquals(value, TimeUnit.HOURS.toMillis(1));
    }


    @After
    public void after() {
    }

}
