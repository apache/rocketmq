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

/**
 * Async batching service for the RocksDB ConsumeQueue.
 *
 * <p>{@link DispatchRequest}s produced by the CommitLog dispatch are
 * queued into an in-memory buffer and consumed in batches by a single
 * background thread, which delegates to
 * {@link RocksDBConsumeQueueStore#putMessagePosition}. The batching
 * reduces the per-message RocksDB write amplification by amortizing
 * the WriteBatch commit cost across up to
 * {@value #PREFERRED_DISPATCH_REQUEST_COUNT} requests.
 *
 * <p>The thread stays parked on a countdown latch and is woken by
 * {@link #putRequest}; if no requests arrive within
 * {@link #waitForRunning(long) waitForRunning(10)} the loop iterates
 * and the buffer is drained on the next signal.
 */
public class RocksGroupCommitService extends ServiceThread {

    /**
     * Upper bound on the in-memory buffer used to absorb bursts between
     * the CommitLog dispatch thread and this commit thread. When full,
     * {@link #putRequest} blocks for up to 3s per retry before logging a
     * warning.
     */
    private static final int MAX_BUFFER_SIZE = 100_000;

    /**
     * Target batch size that triggers {@link #groupCommit} when the
     * request list is at least this large. Larger batches reduce
     * WriteBatch commit overhead but increase commit latency.
     */
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

    /**
     * Enqueue a single dispatch request. Blocks (with 3-second retries)
     * if the buffer is full, then wakes the commit thread.
     */
    public void putRequest(final DispatchRequest request) throws InterruptedException {
        while (!buffer.offer(request, 3, TimeUnit.SECONDS)) {
            log.warn("RocksGroupCommitService#buffer is full, 3s elapsed before space becomes available");
        }
        this.wakeup();
    }

    /**
     * Drain the buffer into {@link #requests}, then flush in
     * {@link #PREFERRED_DISPATCH_REQUEST_COUNT}-sized batches. The
     * inner loop continues polling while the buffer is non-empty,
     * triggering an early flush when the batch is full or when the
     * poll returns null (buffer temporarily empty).
     */
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

    /**
     * Hand the accumulated batch to the store. The store clears
     * {@link #requests} on success via
     * {@link RocksDBConsumeQueueStore#notifyMessageArriveAndClear}.
     * On {@link RocksDBException} the loop retries forever (until the
     * store shuts down), letting the store's internal retry logic
     * recover from transient errors.
     */
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
