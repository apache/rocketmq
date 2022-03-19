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

package org.apache.rocketmq.apis.retry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import java.time.Duration;
import java.util.Random;

/**
 * The {@link BackoffRetryPolicy} defines a policy to do more attempts when failure is encountered, mainly refer to
 * <a href="https://github.com/grpc/proposal/blob/master/A6-client-retries.md">gRPC Retry Design</a>.
 */
public class BackoffRetryPolicy implements RetryPolicy {
    public static BackOffRetryPolicyBuilder newBuilder() {
        return new BackOffRetryPolicyBuilder();
    }

    private final Random random;
    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final int backoffMultiplier;

    public BackoffRetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff, int backoffMultiplier) {
        checkArgument(maxBackoff.compareTo(initialBackoff) <= 0, "initialBackoff should not be minor than maxBackoff");
        checkArgument(maxAttempts > 0, "maxAttempts must be positive");
        this.random = new Random();
        this.maxAttempts = maxAttempts;
        this.initialBackoff = checkNotNull(initialBackoff, "initialBackoff should not be null");
        this.maxBackoff = maxBackoff;
        this.backoffMultiplier = backoffMultiplier;
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    @Override
    public Duration getNextAttemptDelay(int attempt) {
        checkArgument(attempt > 0, "attempt must be positive");
        int randomNumberBound = Math.min(initialBackoff.getNano() * (backoffMultiplier ^ (attempt - 1)),
            maxBackoff.getNano());
        return Duration.ofNanos(random.nextInt(randomNumberBound));
    }

    public Duration getInitialBackoff() {
        return initialBackoff;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public int getBackoffMultiplier() {
        return backoffMultiplier;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("maxAttempts", maxAttempts)
            .add("initialBackoff", initialBackoff)
            .add("maxBackoff", maxBackoff)
            .add("backoffMultiplier", backoffMultiplier)
            .toString();
    }
}
