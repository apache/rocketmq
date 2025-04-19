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
package org.apache.rocketmq.client.lock;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadWriteCASLock {
    //true : can lock ; false : not lock
    private final AtomicBoolean writeLock = new AtomicBoolean(true);

    private final AtomicInteger readLock = new AtomicInteger(0);

    public void acquireWriteLock() {
        boolean isLock = false;
        do {
            isLock = writeLock.compareAndSet(true, false);
        } while (!isLock);

        do {
            isLock = readLock.get() == 0;
        } while (!isLock);
    }

    public void releaseWriteLock() {
        this.writeLock.compareAndSet(false, true);
    }

    public void acquireReadLock() {
        boolean isLock = false;
        do {
            isLock = writeLock.get();
        } while (!isLock);
        readLock.getAndIncrement();
    }

    public void releaseReadLock() {
        this.readLock.getAndDecrement();
    }

    public boolean getWriteLock() {
        return this.writeLock.get() && this.readLock.get() == 0;
    }

    public boolean getReadLock() {
        return this.writeLock.get();
    }

}
