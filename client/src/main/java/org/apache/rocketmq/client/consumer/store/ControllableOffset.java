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

/**
 * The ControllableOffset class encapsulates a thread-safe offset value that can be
 * updated atomically. Additionally, this class allows for the offset to be "frozen,"
 * which prevents further updates after the freeze operation has been performed.
 * <p>
 * Concurrency Scenarios:
 * If {@code updateAndFreeze} is called before any {@code update} operations, it sets
 * {@code allowToUpdate} to false and updates the offset to the target value specified.
 * After this operation, further invocations of {@code update} will not affect the offset,
 * as it is considered frozen.
 * <p>
 * If {@code update} is in progress while {@code updateAndFreeze} is invoked concurrently,
 * the final outcome depends on the sequence of operations:
 * 1. If {@code update}'s atomic update operation completes before {@code updateAndFreeze},
 * the latter will overwrite the offset and set {@code allowToUpdate} to false,
 * preventing any further updates.
 * 2. If {@code updateAndFreeze} executes before the {@code update} finalizes its operation,
 * the ongoing {@code update} will not proceed with its changes. The {@link AtomicLong#getAndUpdate}
 * method used in both operations ensures atomicity and respects the final state imposed by
 * {@code updateAndFreeze}, even if the {@code update} function has already begun.
 * <p>
 * In essence, once the {@code updateAndFreeze} operation is executed, the offset value remains
 * immutable to any subsequent {@code update} calls due to the immediate visibility of the
 * {@code allowToUpdate} state change, courtesy of its volatile nature.
 * <p>
 * The combination of an AtomicLong for the offset value and a volatile boolean flag for update
 * control provides a reliable mechanism for managing offset values in concurrent environments.
 */
public class ControllableOffset {
    // Holds the current offset value in an atomic way.
    private final AtomicLong value;
    // Controls whether updates to the offset are allowed.
    private volatile boolean allowToUpdate;

    public ControllableOffset(long value) {
        this.value = new AtomicLong(value);
        this.allowToUpdate = true;
    }

    /**
     * Attempts to update the offset to the target value. If increaseOnly is true,
     * the offset will not be decreased. The update operation is atomic and thread-safe.
     * The operation will respect the current allowToUpdate state, and if the offset
     * has been frozen by a previous call to {@link #updateAndFreeze(long)},
     * this method will not update the offset.
     *
     * @param target       the new target offset value.
     * @param increaseOnly if true, the offset will only be updated if the target value
     *                     is greater than the current value.
     */
    public void update(long target, boolean increaseOnly) {
        if (allowToUpdate) {
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
    }

    /**
     * Overloaded method for updating the offset value unconditionally.
     *
     * @param target The new target value for the offset.
     */
    public void update(long target) {
        update(target, false);
    }

    /**
     * Freezes the offset at the target value provided. Once frozen, the offset
     * cannot be updated by subsequent calls to {@link #update(long, boolean)}.
     * This method will set allowToUpdate to false and then update the offset,
     * ensuring the new value is the final state of the offset.
     *
     * @param target the new target offset value to freeze at.
     */
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
