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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CollisionRetreatLock implements AdaptiveLock {

    private AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    private int optimalDegree;

    private int maxOptimalDegree;

    private List<AtomicInteger> numberOfRetreat;

    public CollisionRetreatLock() {
        this.optimalDegree = 1000;
        this.maxOptimalDegree = 10000;

        this.numberOfRetreat = new ArrayList<>(2);
        this.numberOfRetreat.add(new AtomicInteger(0));
        this.numberOfRetreat.add(new AtomicInteger(0));
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
            this.numberOfRetreat.get(LocalTime.now().getSecond() % 2).incrementAndGet();
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

    public int getNumberOfRetreat(int pos) {
        return numberOfRetreat.get(pos).get();
    }

    public void setNumberOfRetreat(int numberOfRetreat, int pos) {
        this.numberOfRetreat.get(pos).set(numberOfRetreat);
    }

    public boolean isAdapt() {
        return this.optimalDegree < this.maxOptimalDegree;
    }

    public void adapt() {
        //TODO adapt
    }
}
