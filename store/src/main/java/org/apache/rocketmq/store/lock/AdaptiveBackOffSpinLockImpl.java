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

public class AdaptiveBackOffSpinLockImpl implements AdaptiveBackOffSpinLock {
    private AdaptiveBackOffSpinLock adaptiveLock;
    //state
    private AtomicBoolean state = new AtomicBoolean(true);

    private final static float SWAP_SPIN_LOCK_RATIO = 0.8f;

    private final static int SPIN_LOCK_ADAPTIVE_RATIO = 4;

    private final static int BASE_SWAP_LOCK_RATIO = 320;

    private Map<String, AdaptiveBackOffSpinLock> locks;

    private final List<AtomicInteger> tpsTable;

    private int swapCriticalPoint;

    private AtomicInteger currentThreadNum = new AtomicInteger(0);

    private AtomicBoolean isOpen = new AtomicBoolean(true);

    public AdaptiveBackOffSpinLockImpl() {
        this.locks = new HashMap<>();
        this.locks.put("Reentrant", new PutMessageReentrantLock());
        this.locks.put("BackOff", new BackOffSpinLock());

        this.tpsTable = new ArrayList<>(2);
        this.tpsTable.add(new AtomicInteger(0));
        this.tpsTable.add(new AtomicInteger(0));

        adaptiveLock = this.locks.get("BackOff");
    }

    @Override
    public void lock() {
        this.tpsTable.get(LocalTime.now().getSecond() % 2).getAndIncrement();
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
        int slot = 1 - LocalTime.now().getSecond() % 2;
        int tps = this.tpsTable.get(slot).get() + 1;
        this.tpsTable.get(slot).set(-1);
        if (tps == 0) {
            return;
        }

        if (this.adaptiveLock instanceof BackOffSpinLock) {
            BackOffSpinLock lock = (BackOffSpinLock) this.adaptiveLock;
            if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO >= tps) {
                if (lock.isAdapt()) {
                    lock.adapt(true);
                } else {
                    this.swapCriticalPoint = tps * currentThreadNum.get();
                    needSwap = true;
                }
            } else if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO * SPIN_LOCK_ADAPTIVE_RATIO <= tps) {
                lock.adapt(false);
            }
            lock.setNumberOfRetreat(slot, 0);
        } else {
            if (tps * currentThreadNum.get() <= this.swapCriticalPoint * SWAP_SPIN_LOCK_RATIO) {
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
                    if (this.adaptiveLock instanceof BackOffSpinLock) {
                        this.adaptiveLock = this.locks.get("Reentrant");
                    } else {
                        this.adaptiveLock = this.locks.get("BackOff");
                        ((BackOffSpinLock) this.adaptiveLock).adapt(false);
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
                adaptiveLock = this.locks.get("BackOff");
            } else {
                adaptiveLock = !isUseReentrantLock ? this.locks.get("BackOff") : this.locks.get("Reentrant");
            }
            state.set(true);
        }
    }

    public List<AdaptiveBackOffSpinLock> getLocks() {
        return (List<AdaptiveBackOffSpinLock>) this.locks.values();
    }

    public void setLocks(Map<String, AdaptiveBackOffSpinLock> locks) {
        this.locks = locks;
    }

    public boolean getState() {
        return this.state.get();
    }

    public void setState(boolean state) {
        this.state.set(state);
    }

    public AdaptiveBackOffSpinLock getAdaptiveLock() {
        return adaptiveLock;
    }

    public List<AtomicInteger> getTpsTable() {
        return tpsTable;
    }

    public void setTpsSwapCriticalPoint(int swapCriticalPoint) {
        this.swapCriticalPoint = swapCriticalPoint;
    }

    public int getTpsSwapCriticalPoint() {
        return swapCriticalPoint;
    }

    public boolean isOpen() {
        return this.isOpen.get();
    }

    public void setOpen(boolean open) {
        this.isOpen.set(open);
    }
}
