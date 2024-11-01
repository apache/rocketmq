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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AdaptiveBackOffSpinLockImpl implements AdaptiveBackOffSpinLock {
    private AdaptiveBackOffSpinLock adaptiveLock;
    //state
    private AtomicBoolean state = new AtomicBoolean(true);

    // Used to determine the switchover between a mutex lock and a spin lock
    private final static float SWAP_SPIN_LOCK_RATIO = 0.8f;

    // It is used to adjust the spin number K of the escape spin lock
    // When (retreat number / TPS) <= (1 / BASE_SWAP_ADAPTIVE_RATIO * SPIN_LOCK_ADAPTIVE_RATIO), K is decreased
    private final static int SPIN_LOCK_ADAPTIVE_RATIO = 4;

    // It is used to adjust the spin number K of the escape spin lock
    // When (retreat number / TPS) >= (1 / BASE_SWAP_ADAPTIVE_RATIO), K is increased
    private final static int BASE_SWAP_LOCK_RATIO = 320;

    private final static String BACK_OFF_SPIN_LOCK = "SpinLock";

    private final static String REENTRANT_LOCK = "ReentrantLock";

    private Map<String, AdaptiveBackOffSpinLock> locks;

    private final List<AtomicInteger> tpsTable;

    private final List<Map<Thread, Byte>> threadTable;

    private int swapCriticalPoint;

    private AtomicInteger currentThreadNum = new AtomicInteger(0);

    private AtomicBoolean isOpen = new AtomicBoolean(true);

    public AdaptiveBackOffSpinLockImpl() {
        this.locks = new HashMap<>();
        this.locks.put(REENTRANT_LOCK, new BackOffReentrantLock());
        this.locks.put(BACK_OFF_SPIN_LOCK, new BackOffSpinLock());

        this.threadTable = new ArrayList<>(2);
        this.threadTable.add(new ConcurrentHashMap<>());
        this.threadTable.add(new ConcurrentHashMap<>());

        this.tpsTable = new ArrayList<>(2);
        this.tpsTable.add(new AtomicInteger(0));
        this.tpsTable.add(new AtomicInteger(0));

        adaptiveLock = this.locks.get(BACK_OFF_SPIN_LOCK);
    }

    @Override
    public void lock() {
        int slot = LocalTime.now().getSecond() % 2;
        this.threadTable.get(slot).putIfAbsent(Thread.currentThread(), Byte.MAX_VALUE);
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
            // Avoid frequent adjustment of K, and make a reasonable range through experiments
            // reasonable range : (retreat number / TPS) > (1 / BASE_SWAP_ADAPTIVE_RATIO * SPIN_LOCK_ADAPTIVE_RATIO) &&
            // (retreat number / TPS) < (1 / BASE_SWAP_ADAPTIVE_RATIO)
            if (lock.getNumberOfRetreat(slot) * BASE_SWAP_LOCK_RATIO >= tps) {
                if (lock.isAdapt()) {
                    lock.adapt(true);
                } else {
                    // It is used to switch between mutex lock and spin lock
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
                // Ensures that no threads are in contention locks as well as in critical zones
                int currentThreadNum;
                do {
                    currentThreadNum = this.currentThreadNum.get();
                } while (currentThreadNum != 0);

                try {
                    if (this.adaptiveLock instanceof BackOffSpinLock) {
                        this.adaptiveLock = this.locks.get(REENTRANT_LOCK);
                    } else {
                        this.adaptiveLock = this.locks.get(BACK_OFF_SPIN_LOCK);
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

    public void setSwapCriticalPoint(int swapCriticalPoint) {
        this.swapCriticalPoint = swapCriticalPoint;
    }

    public int getSwapCriticalPoint() {
        return swapCriticalPoint;
    }

    public boolean isOpen() {
        return this.isOpen.get();
    }

    public void setOpen(boolean open) {
        this.isOpen.set(open);
    }
}
