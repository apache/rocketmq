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

public class AdaptiveLockImpl implements AdaptiveLock {
    private AdaptiveLock adaptiveLock;

    //state
    private AtomicBoolean state = new AtomicBoolean(true);

    private Map<String, AdaptiveLock> locks;

    private final List<AtomicInteger> tpsTable;

    public AdaptiveLockImpl() {
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

        this.adaptiveLock.lock();
    }

    @Override
    public void unlock() {
        boolean state;
        do {
            state = this.state.get();
        } while (!state);

        this.adaptiveLock.unlock();
        swap();
    }

    @Override
    public void update(MessageStoreConfig messageStoreConfig) {
        this.adaptiveLock.update(messageStoreConfig);
    }

    @Override
    public void swap() {
        boolean needSwap = false;
        int slot = LocalTime.now().getSecond() % 2 - 1 >= 0 ? 0 : 1;
        int tps = this.tpsTable.get(slot).get();
        this.tpsTable.get(slot).set(-1);
        if (tps == -1) {
            return;
        }
        if (tps > 50000) {
            if (this.adaptiveLock instanceof CollisionRetreatLock) {
                needSwap = true;
            }
        } else {
            if (this.adaptiveLock instanceof PutMessageReentrantLock) {
                needSwap = true;
            }
        }

        if (needSwap) {
            if (this.state.compareAndSet(true, false)) {
                this.adaptiveLock.lock();
                if (this.adaptiveLock instanceof CollisionRetreatLock) {
                    this.adaptiveLock = this.locks.get("Reentrant");
                } else {
                    this.adaptiveLock = this.locks.get("Collision");
                }
                try {
                    this.adaptiveLock.unlock();
                } catch (Exception ignore) {
                }
                this.state.compareAndSet(false, true);
            }
        }
    }

    public List<AdaptiveLock> getLocks() {
        return (List<AdaptiveLock>) this.locks.values();
    }

    public void setLocks(Map<String, AdaptiveLock> locks) {
        this.locks = locks;
    }

    public boolean getState() {
        return this.state.get();
    }

    public void setState(boolean state) {
        this.state.set(state);
    }

    public AdaptiveLock getAdaptiveLock() {
        return this.adaptiveLock;
    }

    public List<AtomicInteger> getTpsTable() {
        return this.tpsTable;
    }
}
