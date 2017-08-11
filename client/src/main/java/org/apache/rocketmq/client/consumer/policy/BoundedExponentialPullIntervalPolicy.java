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

package org.apache.rocketmq.client.consumer.policy;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.PullIntervalPolicy;
import org.apache.rocketmq.client.consumer.PullStatus;

public class BoundedExponentialPullIntervalPolicy implements PullIntervalPolicy {

    private final int base;
    private final int max;
    private AtomicInteger count;

    private volatile long pullInterval;

    public BoundedExponentialPullIntervalPolicy(int base, int max) {
        count = new AtomicInteger();
        if (base > 0) {
            this.base = base;
        } else {
            this.base = 1000;
        }

        this.max = max;
    }

    @Override
    public void update(PullStatus status) {
        switch (status) {
            case FOUND:
            case OFFSET_ILLEGAL:
                count.set(0);
                pullInterval = 0;
                break;

            case NO_MATCHED_MSG:
            case NO_NEW_MSG:
                pullInterval = base * (1 << count.get());
                if (pullInterval > max) {
                    pullInterval = max;
                }
                break;

            default:
                break;

        }

    }

    @Override
    public int getPullInterval() {
        return (int)pullInterval;
    }
}
