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
package org.apache.rocketmq.broker.pop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;

/**
 * One owner for POP KV mutations and cache state transitions. Submission never waits for disk.
 * Requests retain their order and are never split across WriteBatches.
 */
class PopConsumerWriteBatchService extends ServiceThread {
    private static final int MAX_BATCH_REQUESTS = 128;
    private static final long MAX_BATCH_BYTES = 1024 * 1024;

    interface BatchWriter {
        void write(List<Mutation> mutations) throws Exception;
    }

    static class Mutation {
        final byte[] key;
        final byte[] value;

        Mutation(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    enum Type {
        WRITE, POP, ACK, CHANGE, TRANSFER, CLEAR
    }

    static class Request {
        final Type type;
        final PopConsumerCache cache;
        final List<PopConsumerRecord> records;
        final List<Mutation> mutations;
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final List<PopConsumerRecord> expired = new ArrayList<>();
        final List<String> offlineQueues = new ArrayList<>();
        long bytes;
        boolean asyncCompletion = true;
        boolean cachePop;
        String cacheKey;

        Request(Type type, PopConsumerCache cache, List<PopConsumerRecord> records) {
            this.type = type;
            this.cache = cache;
            this.records = records;
            this.mutations = new ArrayList<>();
        }

        Request(List<Mutation> mutations, boolean asyncCompletion) {
            this.type = Type.WRITE;
            this.cache = null;
            this.records = Collections.emptyList();
            this.mutations = mutations;
            this.asyncCompletion = asyncCompletion;
        }

        void calculateBytes() {
            bytes = mutations.stream().mapToLong(m -> m.key.length + (m.value == null ? 0L : m.value.length)).sum();
            for (PopConsumerRecord record : records) {
                bytes += record.getKeyBytes().length + record.getValueBytes().length;
            }
        }
    }

    private final Object admissionLock = new Object();
    private final LinkedBlockingQueue<Request> queue = new LinkedBlockingQueue<>();
    private final BatchWriter writer;
    private final ExecutorService completions;
    private boolean accepting;

    PopConsumerWriteBatchService(BatchWriter writer) {
        this(writer, 4);
    }

    PopConsumerWriteBatchService(BatchWriter writer, int completionThreads) {
        this.writer = writer;
        this.completions = Executors.newFixedThreadPool(completionThreads,
            new ThreadFactoryImpl("PopKVCompletion_"));
    }

    @Override
    public String getServiceName() {
        return "PopKVGroupCommit";
    }

    @Override
    public synchronized void start() {
        if (completions.isShutdown()) {
            throw new IllegalStateException("POP KV writer already shut down");
        }
        synchronized (admissionLock) {
            super.start();
            accepting = true;
        }
    }

    CompletableFuture<Void> submit(List<Mutation> mutations) {
        return submit(mutations, true);
    }

    // Used only by blocking compatibility methods; their private futures have no continuations.
    CompletableFuture<Void> submit(List<Mutation> mutations, boolean asyncCompletion) {
        return submit(new Request(mutations, asyncCompletion));
    }

    CompletableFuture<Void> submit(Request request) {
        request.calculateBytes();
        synchronized (admissionLock) {
            if (!accepting || isStopped()) {
                request.future.completeExceptionally(new RejectedExecutionException("POP KV writer stopped"));
                return request.future;
            }
            queue.add(request);
        }
        wakeup();
        return request.future;
    }

    @Override
    public void run() {
        while (!isStopped() || !queue.isEmpty()) {
            Request first = queue.poll();
            if (first == null) {
                waitForRunning(1000);
                continue;
            }
            List<Request> requests = new ArrayList<>();
            List<Mutation> mutations = new ArrayList<>();
            requests.add(first);
            long bytes = first.bytes;
            // Commit each operation type before inspecting cache state for the next type.
            // TRANSFER and CLEAR run alone; ordinary ACKs and CHANGEs still group commit.
            while (first.type != Type.TRANSFER && first.type != Type.CLEAR && requests.size() < MAX_BATCH_REQUESTS) {
                Request next = queue.peek();
                if (next == null || next.type != first.type || bytes + next.bytes > MAX_BATCH_BYTES) {
                    break;
                }
                queue.poll();
                requests.add(next);
                bytes += next.bytes;
            }
            List<Request> prepared = new ArrayList<>();
            for (Request request : requests) {
                try {
                    prepare(request);
                    mutations.addAll(request.mutations);
                    prepared.add(request);
                } catch (Throwable t) {
                    finish(request, t);
                }
            }
            Throwable failure = null;
            try {
                if (!mutations.isEmpty()) {
                    writer.write(mutations);
                }
            } catch (Throwable t) {
                failure = t;
            }
            for (Request request : prepared) {
                Throwable result = failure;
                if (result == null) {
                    try {
                        // Publish cache state before preparing the next (possibly dependent) batch.
                        commit(request);
                    } catch (Throwable t) {
                        result = t;
                    }
                }
                finish(request, result);
            }
        }
    }

    private void prepare(Request request) {
        switch (request.type) {
            case POP:
                request.cachePop = !request.cache.isCacheFull();
                if (!request.cachePop) {
                    for (PopConsumerRecord record : request.records) {
                        request.mutations.add(new Mutation(record.getKeyBytes(), record.getValueBytes()));
                    }
                }
                break;
            case ACK:
                for (PopConsumerRecord record : request.records) {
                    if (!request.cache.contains(record)) {
                        request.mutations.add(new Mutation(record.getKeyBytes(), null));
                    }
                }
                break;
            case TRANSFER:
                for (PopConsumerRecord record : request.cache.prepareTransfer(request.expired, request.offlineQueues)) {
                    request.mutations.add(new Mutation(record.getKeyBytes(), record.getValueBytes()));
                }
                break;
            default:
                break;
        }
    }

    private void commit(Request request) {
        switch (request.type) {
            case POP:
                if (request.cachePop) {
                    request.cache.writeRecords(request.records);
                }
                break;
            case ACK:
            case CHANGE:
                request.cache.deleteRecords(request.records);
                break;
            case TRANSFER:
                request.cache.completeTransfer(request.offlineQueues);
                break;
            case CLEAR:
                request.cache.removeRecords(request.cacheKey);
                break;
            default:
                break;
        }
    }

    private void finish(Request request, Throwable result) {
        if (request.asyncCompletion) {
            // User continuations run independently, including requests from the same batch.
            completions.execute(() -> complete(request, result));
        } else {
            // Private blocking futures have no user continuations.
            complete(request, result);
        }
    }

    private void complete(Request request, Throwable failure) {
        if (failure == null) {
            request.future.complete(null);
        } else {
            request.future.completeExceptionally(failure);
        }
    }

    @Override
    public synchronized void shutdown(boolean interrupt) {
        synchronized (admissionLock) {
            accepting = false;
            super.makeStop();
        }
        // Drain before completing ServiceThread shutdown: its timed join must not allow
        // RocksDB to close while a native write is still running. Never interrupt the writer.
        boolean interrupted = false;
        while (thread != null && thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        super.shutdown(false);
        completions.shutdown();
        boolean terminated = false;
        while (!terminated) {
            try {
                terminated = completions.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
