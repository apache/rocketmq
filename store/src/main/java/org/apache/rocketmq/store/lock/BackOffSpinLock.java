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

import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BackOffSpinLock implements AdaptiveBackOffLock {

    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    private int optimalDegree;

    private final static int INITIAL_DEGREE = 1000;

    private final static int MAX_OPTIMAL_DEGREE = 10000;

    private final List<AtomicInteger> numberOfRetreat;

    public BackOffSpinLock() {
        this.optimalDegree = INITIAL_DEGREE;

        numberOfRetreat = new ArrayList<>(2);
        numberOfRetreat.add(new AtomicInteger(0));
        numberOfRetreat.add(new AtomicInteger(0));
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
            numberOfRetreat.get(LocalTime.now().getSecond() % 2).getAndIncrement();
            Thread.yield();
        }
    }

    @Override
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }

    @Override
    public void update(MessageStoreConfig messageStoreConfig) {
        this.optimalDegree = messageStoreConfig.getSpinLockCollisionRetreatOptimalDegree();
    }

    public int getOptimalDegree() {
        return this.optimalDegree;
    }

    public void setOptimalDegree(int optimalDegree) {
        this.optimalDegree = optimalDegree;
    }

    public boolean isAdapt() {
        return optimalDegree < MAX_OPTIMAL_DEGREE;
    }

    public synchronized void adapt(boolean isRise) {
        if (isRise) {
            if (optimalDegree * 2 <= MAX_OPTIMAL_DEGREE) {
                optimalDegree *= 2;
            } else {
                if (optimalDegree + INITIAL_DEGREE <= MAX_OPTIMAL_DEGREE) {
                    optimalDegree += INITIAL_DEGREE;
                }
            }
        } else {
            if (optimalDegree / 2 >= INITIAL_DEGREE) {
                optimalDegree /= 2;
            } else {
                if (optimalDegree > 2 * INITIAL_DEGREE) {
                    optimalDegree -= MAX_OPTIMAL_DEGREE;
                }
            }
        }
    }

    public int getNumberOfRetreat(int pos) {
        return numberOfRetreat.get(pos).get();
    }

    public void setNumberOfRetreat(int pos, int size) {
        this.numberOfRetreat.get(pos).set(size);
    }
}
