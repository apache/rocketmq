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

import org.apache.rocketmq.store.PutMessageReentrantLock;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AdaptiveBackOffLockImpl implements AdaptiveBackOffLock {
    private AdaptiveBackOffLock adaptiveLock;

    //state
    private AtomicBoolean state = new AtomicBoolean(true);

    private Map<String, AdaptiveBackOffLock> locks;

    private final List<AtomicInteger> tpsTable;

    private int tpsSwapCriticalPoint;

    private AtomicInteger currentThreadNum = new AtomicInteger(0);

    private AtomicBoolean isOpen = new AtomicBoolean(true);

    public AdaptiveBackOffLockImpl() {
        this.locks = new HashMap<>();
        this.locks.put("Reentrant", new PutMessageReentrantLock());
        this.locks.put("Collision", new CollisionRetreatLock());

        this.tpsTable = new ArrayList<>(2);
        this.tpsTable.add(new AtomicInteger(0));
        this.tpsTable.add(new AtomicInteger(0));

        adaptiveLock = this.locks.get("Collision");
    }

    @Override
    public void lock() {
        tpsTable.get(LocalTime.now().getSecond() % 2).getAndIncrement();
        boolean state;
        do {
            state = this.state.get();
        } while (!state);

        currentThreadNum.incrementAndGet();
        this.adaptiveLock.lock();
    }

    @Override
    public void unlock() {
        this.adaptiveLock.unlock();
        currentThreadNum.decrementAndGet();
        if (isOpen.get()) {
            swap();
        }
    }

    @Override
    public void update(MessageStoreConfig messageStoreConfig) {
        this.adaptiveLock.update(messageStoreConfig);
    }

    @Override
    public void swap() {
        if (!this.state.get()) {
            return;
        }
        boolean needSwap = false;
        int slot = LocalTime.now().getSecond() % 2 - 1 >= 0 ? 0 : 1;
        int tps = this.tpsTable.get(slot).get() + 1;
        this.tpsTable.get(slot).set(-1);
        if (tps == 0) {
            return;
        }

        if (this.adaptiveLock instanceof CollisionRetreatLock) {
            CollisionRetreatLock lock = (CollisionRetreatLock) this.adaptiveLock;
            int base = Math.min(200 + tps / 200, 500);
            if (lock.getNumberOfRetreat(slot) * base >= tps) {
                if (lock.isAdapt()) {
                    lock.adapt(true);
                } else {
                    this.tpsSwapCriticalPoint = tps;
                    needSwap = true;
                }
            } else if (lock.getNumberOfRetreat(slot) * base * 3 / 2 <= tps) {
                lock.adapt(false);
            }
            lock.setNumberOfRetreat(slot, 0);
        } else {
            if (tps <= this.tpsSwapCriticalPoint * 4 / 5) {
                needSwap = true;
            }
        }

        if (needSwap) {
            if (this.state.compareAndSet(true, false)) {
                int currentThreadNum;
                do {
                    currentThreadNum = this.currentThreadNum.get();
                } while (currentThreadNum != 0);

                try {
                    if (this.adaptiveLock instanceof CollisionRetreatLock) {
                        this.adaptiveLock = this.locks.get("Reentrant");
                    } else {
                        this.adaptiveLock = this.locks.get("Collision");
                        ((CollisionRetreatLock) this.adaptiveLock).adapt(false);
                    }
                } catch (Exception e) {
                    //ignore
                } finally {
                    this.state.compareAndSet(false, true);
                }
            }
        }
    }

    @Override
    public void isOpen(boolean open, boolean isUseReentrantLock) {
        if (open != isOpen.get()) {
            isOpen.set(open);
            boolean success = false;
            do {
                success = this.state.compareAndSet(true, false);
            } while (!success);

            int currentThreadNum;
            do {
                currentThreadNum = this.currentThreadNum.get();
            } while (currentThreadNum != 0);

            if (open) {
                adaptiveLock = this.locks.get("Collision");
            } else {
                adaptiveLock = !isUseReentrantLock ? this.locks.get("Collision") : this.locks.get("Reentrant");
            }
            state.set(true);
        }
    }

    public List<AdaptiveBackOffLock> getLocks() {
        return (List<AdaptiveBackOffLock>) this.locks.values();
    }

    public void setLocks(Map<String, AdaptiveBackOffLock> locks) {
        this.locks = locks;
    }

    public boolean getState() {
        return this.state.get();
    }

    public void setState(boolean state) {
        this.state.set(state);
    }

    public AdaptiveBackOffLock getAdaptiveLock() {
        return adaptiveLock;
    }

    public List<AtomicInteger> getTpsTable() {
        return tpsTable;
    }

    public void setTpsSwapCriticalPoint(int tpsSwapCriticalPoint) {
        this.tpsSwapCriticalPoint = tpsSwapCriticalPoint;
    }

    public int getTpsSwapCriticalPoint() {
        return tpsSwapCriticalPoint;
    }
}
