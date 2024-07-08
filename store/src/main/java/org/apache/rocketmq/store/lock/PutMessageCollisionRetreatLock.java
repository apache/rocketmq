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
package org.apache.rocketmq.store.lock;


import org.apache.rocketmq.store.PutMessageLock;

import java.util.concurrent.atomic.AtomicBoolean;

public class PutMessageCollisionRetreatLock implements PutMessageLock {

    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    private int optimalDegree;

    public PutMessageCollisionRetreatLock(int optimalDegree) {
        this.optimalDegree = optimalDegree;
    }

    @Override
    public void lock() {
        int spinDegree = this.optimalDegree;
        while (true) {
            for (int i = 0; i < spinDegree; i++) {
                if (this.putMessageSpinLock.compareAndSet(true, false)) {
                    return;
                }
            }
            Thread.yield();
        }
    }

    @Override
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}
