/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.subscription;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class CustomizedRetryPolicy {
    // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    private long[] next = new long[]{
        TimeUnit.SECONDS.toMillis(1),
        TimeUnit.SECONDS.toMillis(5),
        TimeUnit.SECONDS.toMillis(10),
        TimeUnit.SECONDS.toMillis(30),
        TimeUnit.MINUTES.toMillis(1),
        TimeUnit.MINUTES.toMillis(2),
        TimeUnit.MINUTES.toMillis(3),
        TimeUnit.MINUTES.toMillis(4),
        TimeUnit.MINUTES.toMillis(5),
        TimeUnit.MINUTES.toMillis(6),
        TimeUnit.MINUTES.toMillis(7),
        TimeUnit.MINUTES.toMillis(8),
        TimeUnit.MINUTES.toMillis(9),
        TimeUnit.MINUTES.toMillis(10),
        TimeUnit.MINUTES.toMillis(20),
        TimeUnit.MINUTES.toMillis(30),
        TimeUnit.HOURS.toMillis(1),
        TimeUnit.HOURS.toMillis(2)
    };

    public long[] getNext() {
        return next;
    }

    public void setNext(long[] next) {
        this.next = next;
    }

    @Override
    public String toString() {
        return "CustomizedRetryPolicy{" +
            "next=" + Arrays.toString(next) +
            '}';
    }
}
