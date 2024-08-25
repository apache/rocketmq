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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdaptiveLockImpl implements AdaptiveLock {

    private AdaptiveLock adaptiveLock;

    //state
    private AtomicBoolean state = new AtomicBoolean(true);

    private List<AdaptiveLock> locks;

    public AdaptiveLockImpl() {
        this.locks = new LinkedList<>();
        this.locks.add(new CollisionRetreatLock());
        this.locks.add(new PutMessageReentrantLock());
        adaptiveLock = new CollisionRetreatLock();
    }

    @Override
    public void lock() {
        while (this.state.get()) {
            this.adaptiveLock.lock();
        }
    }

    @Override
    public void unlock() {
        while (this.state.get()) {
            this.adaptiveLock.unlock();
        }
    }

    @Override
    public void update(MessageStoreConfig messageStoreConfig) {
        this.adaptiveLock.update(messageStoreConfig);
    }

    @Override
    public void swap() {
        if (this.state.compareAndSet(true, false)) {
            //change adaptiveLock

            this.state.compareAndSet(false, true);
        }
    }

    public List<AdaptiveLock> getLocks() {
        return this.locks;
    }

    public void setLocks(List<AdaptiveLock> locks) {
        this.locks = locks;
    }

    public boolean getState() {
        return this.state.get();
    }

    public void setState(boolean state) {
        this.state.set(state);
    }
}
