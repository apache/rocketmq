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

package org.apache.rocketmq.thinclient.retry;

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.RetryPolicy;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static apache.rocketmq.v2.RetryPolicy.StrategyCase.CUSTOMIZED_BACKOFF;
import static org.junit.Assert.assertEquals;

public class CustomizedBackoffRetryPolicyTest {

    @Test
    public void testNextAttemptDelay() {
        int maxAttempt = 3;
        Duration duration0 = Duration.ofSeconds(1);
        Duration duration1 = Duration.ofSeconds(2);
        List<Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        final CustomizedBackoffRetryPolicy policy = new CustomizedBackoffRetryPolicy(durations, maxAttempt);
        assertEquals(maxAttempt, policy.getMaxAttempts());
        assertEquals(duration0, policy.getNextAttemptDelay(1));
        assertEquals(duration1, policy.getNextAttemptDelay(2));
        assertEquals(duration1, policy.getNextAttemptDelay(3));
        assertEquals(duration1, policy.getNextAttemptDelay(4));
    }

    @Test
    public void testToProtobuf() {
        int maxAttempt = 3;
        Duration duration0 = Duration.ofSeconds(1);
        Duration duration1 = Duration.ofSeconds(2);
        List<Duration> durations = new ArrayList<>();
        durations.add(duration0);
        durations.add(duration1);
        final CustomizedBackoffRetryPolicy policy = new CustomizedBackoffRetryPolicy(durations, maxAttempt);
        final RetryPolicy protobuf = policy.toProtobuf();
        assertEquals(maxAttempt, protobuf.getMaxAttempts());
        assertEquals(CUSTOMIZED_BACKOFF, protobuf.getStrategyCase());
        final CustomizedBackoff backoff = protobuf.getCustomizedBackoff();
        final List<com.google.protobuf.Duration> next = backoff.getNextList();
        assertEquals(durations.size(), next.size());
        for (int i = 0; i < durations.size(); i++) {
            assertEquals(durations.get(i).toNanos(), Durations.toNanos(next.get(i)));
        }
    }
}