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

package org.apache.rocketmq.proxy.common;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.remoting.protocol.subscription.RetryPolicy;

import java.util.concurrent.TimeUnit;


public class RenewStrategyPolicy implements RetryPolicy {
    // 1m 3m 5m 6m 10m 30m 1h
    private long[] next = new long[]{
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MINUTES.toMillis(3),
            TimeUnit.MINUTES.toMillis(5),
            TimeUnit.MINUTES.toMillis(10),
            TimeUnit.MINUTES.toMillis(30),
            TimeUnit.HOURS.toMillis(1)
    };

    public RenewStrategyPolicy() {
    }

    public RenewStrategyPolicy(long[] next) {
        this.next = next;
    }

    public long[] getNext() {
        return next;
    }

    public void setNext(long[] next) {
        this.next = next;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("next", next)
                .toString();
    }

    @Override
    public long nextDelayDuration(int renewTimes) {
        if (renewTimes < 0) {
            renewTimes = 0;
        }
        int index = renewTimes;
        if (index >= next.length) {
            index = next.length - 1;
        }
        return next[index];
    }
}
