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

import org.apache.rocketmq.client.consumer.PullIntervalPolicy;
import org.apache.rocketmq.client.consumer.PullStatus;

public class LinearPullIntervalPolicy implements PullIntervalPolicy {

    private final int low;
    private final int high;
    private final int delta;

    private volatile int pullInterval;

    public LinearPullIntervalPolicy(int low, int high, int delta) {
        this.low = low;
        this.high = high;
        this.delta = delta;
        pullInterval = low;
    }

    @Override
    public void update(PullStatus status) {
        switch (status) {
            case FOUND:
            case OFFSET_ILLEGAL:
                pullInterval = 0;
                break;
            case NO_NEW_MSG:
            case NO_MATCHED_MSG:
                if (0 == pullInterval) {
                    pullInterval = low;
                } else if (pullInterval + delta > high) {
                    pullInterval = high;
                } else {
                    pullInterval += delta;
                }
                break;

            default:
                break;
        }
    }

    @Override
    public int getPullInterval() {
        return pullInterval;
    }
}
