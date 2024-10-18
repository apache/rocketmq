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

package org.apache.rocketmq.store.queue;

import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.store.DispatchRequest;
import org.rocksdb.RocksDBException;

public class RocksGroupCommitService extends ServiceThread {
    private volatile LinkedList<DispatchRequest> requestsWrite = new LinkedList<>();
    private volatile LinkedList<DispatchRequest> requestsRead = new LinkedList<>();

    private final Lock lock = new ReentrantLock();

    private final RocksDBConsumeQueueStore store;

    public RocksGroupCommitService(RocksDBConsumeQueueStore store) {
        this.store = store;
    }

    @Override
    public String getServiceName() {
        return "RocksGroupCommit";
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);
                this.doCommit();
            } catch (Exception e) {
                log.warn("{} service has exception. ", this.getServiceName(), e);
            }
        }
        log.info("{} service end", this.getServiceName());
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    public void putRequest(final DispatchRequest request) {
        lock.lock();
        try {
            this.requestsWrite.add(request);
        } finally {
            lock.unlock();
        }
        this.wakeup();
    }

    private void swapRequests() {
        lock.lock();
        try {
            LinkedList<DispatchRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        } finally {
            lock.unlock();
        }
    }

    private void doCommit() {
        if (!this.requestsRead.isEmpty()) {
            try {
                store.putMessagePosition(requestsRead);
            } catch (RocksDBException e) {
                log.error("Failed to build consume queue", e);
            }
        }
    }

}
