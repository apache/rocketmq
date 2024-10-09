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
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final List<Set<Thread>> threadTable;

    private int swapCriticalPoint;

    private AtomicInteger currentThreadNum = new AtomicInteger(0);

    private AtomicBoolean isOpen = new AtomicBoolean(true);

    public AdaptiveBackOffSpinLockImpl(PutMessageLock putMessageLock) {
        this.locks = new HashMap<>();
        this.locks.put("Reentrant", new BackOffReentrantLock());
        this.locks.put("Spin", new BackOffSpinLock());
        this.locks.put("putMessage", putMessageLock);

        this.threadTable = new ArrayList<>(2);
        this.threadTable.add(new HashSet<>());
        this.threadTable.add(new HashSet<>());

        this.tpsTable = new ArrayList<>(2);
        this.tpsTable.add(new AtomicInteger(0));
        this.tpsTable.add(new AtomicInteger(0));

        adaptiveLock = this.locks.get("Spin");
    }

    @Override
    public void lock() {
        int slot = LocalTime.now().getSecond() % 2;
        this.threadTable.get(slot).add(Thread.currentThread());
        this.tpsTable.get(slot).getAndIncrement();
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
        int threadNum = this.threadTable.get(slot).size();
        this.tpsTable.get(slot).set(-1);
        this.threadTable.get(slot).clear();
        if (tps == 0) {
            return;
        }

        if (this.adaptiveLock instanceof BackOffSpinLock) {
            BackOffSpinLock lock = (BackOffSpinLock) this.adaptiveLock;
            if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO >= tps) {
                if (lock.isAdapt()) {
                    lock.adapt(true);
                } else {
                    this.swapCriticalPoint = tps * threadNum;
                    needSwap = true;
                }
            } else if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO * SPIN_LOCK_ADAPTIVE_RATIO <= tps) {
                lock.adapt(false);
            }
            lock.setNumberOfRetreat(slot, 0);
        } else {
            if (tps * threadNum <= this.swapCriticalPoint * SWAP_SPIN_LOCK_RATIO) {
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
                        this.adaptiveLock = this.locks.get("Spin");
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
    public void isOpen(boolean open) {
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
                adaptiveLock = this.locks.get("Spin");
            } else {
                adaptiveLock = this.locks.get("putMessage");
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

    public int getSwapCriticalPoint() {
        return swapCriticalPoint;
    }

    public void setSwapSpinLockRatio(int swapCriticalPoint) {
        this.swapCriticalPoint = swapCriticalPoint;
    }
}
