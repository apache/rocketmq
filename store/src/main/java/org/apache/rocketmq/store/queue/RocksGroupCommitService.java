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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.store.DispatchRequest;
import org.rocksdb.RocksDBException;

public class RocksGroupCommitService extends ServiceThread {

    private static final int MAX_BUFFER_SIZE = 100_000;

    private static final int PREFERRED_DISPATCH_REQUEST_COUNT = 256;

    private final LinkedBlockingQueue<DispatchRequest> buffer;

    private final RocksDBConsumeQueueStore store;

    private final List<DispatchRequest> requests = new ArrayList<>(PREFERRED_DISPATCH_REQUEST_COUNT);

    public RocksGroupCommitService(RocksDBConsumeQueueStore store) {
        this.store = store;
        this.buffer = new LinkedBlockingQueue<>(MAX_BUFFER_SIZE);
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

    public void putRequest(final DispatchRequest request) throws InterruptedException {
        while (!buffer.offer(request, 3, TimeUnit.SECONDS)) {
            log.warn("RocksGroupCommitService#buffer is full, 3s elapsed before space becomes available");
        }
        this.wakeup();
    }

    private void doCommit() {
        while (!buffer.isEmpty()) {
            while (true) {
                DispatchRequest dispatchRequest = buffer.poll();
                if (null != dispatchRequest) {
                    requests.add(dispatchRequest);
                }

                if (requests.isEmpty()) {
                    // buffer has been drained
                    break;
                }

                if (null == dispatchRequest || requests.size() >= PREFERRED_DISPATCH_REQUEST_COUNT) {
                    groupCommit();
                }
            }
        }
    }

    private void groupCommit() {
        while (!store.isStopped()) {
            try {
                // putMessagePosition will clear requests after consume queue building completion
                store.putMessagePosition(requests);
                break;
            } catch (RocksDBException e) {
                log.error("Failed to build consume queue in RocksDB", e);
            }
        }
    }

}
