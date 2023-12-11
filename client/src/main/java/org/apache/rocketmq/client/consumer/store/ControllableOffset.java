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

package org.apache.rocketmq.client.consumer.store;

import java.util.concurrent.atomic.AtomicLong;

public class ControllableOffset {
    private final AtomicLong value;
    private volatile boolean allowToUpdate;

    public ControllableOffset(long value) {
        this.value = new AtomicLong(value);
        this.allowToUpdate = true;
    }

    public void update(long target, boolean increaseOnly) {
        value.getAndUpdate(val -> {
            if (allowToUpdate) {
                if (increaseOnly) {
                    return Math.max(target, val);
                } else {
                    return target;
                }
            } else {
                return val;
            }
        });
    }


    public void update(long target) {
        update(target, false);
    }

    public void updateAndFreeze(long target) {
        value.getAndUpdate(val -> {
            allowToUpdate = false;
            return target;
        });
    }

    public long getOffset() {
        return value.get();
    }
}
