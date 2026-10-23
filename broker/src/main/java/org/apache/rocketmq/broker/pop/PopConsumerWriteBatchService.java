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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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

    static class PreparedWrite {
        final List<Mutation> mutations;
        final Runnable committed;

        PreparedWrite(List<Mutation> mutations, Runnable committed) {
            this.mutations = mutations;
            this.committed = committed;
        }
    }

    private static class Request {
        final Supplier<PreparedWrite> prepare;
        final Set<ByteBuffer> keys;
        final boolean barrier;
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final long bytes;
        final boolean asyncCompletion;
        PreparedWrite prepared;

        Request(Supplier<PreparedWrite> prepare, Set<ByteBuffer> keys, boolean barrier,
            long bytes, boolean asyncCompletion) {
            this.prepare = prepare;
            this.keys = keys;
            this.barrier = barrier;
            this.bytes = bytes;
            this.asyncCompletion = asyncCompletion;
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
        long bytes = mutations.stream().mapToLong(m -> m.key.length + (m.value == null ? 0L : m.value.length)).sum();
        return submit(() -> new PreparedWrite(mutations, () -> { }), Collections.emptySet(), false, bytes, asyncCompletion);
    }

    CompletableFuture<Void> submit(Supplier<PreparedWrite> prepare, Set<ByteBuffer> keys,
        boolean barrier, long bytes, boolean asyncCompletion) {
        Request request = new Request(prepare, keys, barrier, bytes, asyncCompletion);
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
            Set<ByteBuffer> keys = new HashSet<>(first.keys);
            long bytes = first.bytes;
            while (!first.barrier && requests.size() < MAX_BATCH_REQUESTS) {
                Request next = queue.peek();
                if (next == null || next.barrier || bytes + next.bytes > MAX_BATCH_BYTES
                    || !Collections.disjoint(keys, next.keys)) {
                    break;
                }
                queue.poll();
                requests.add(next);
                keys.addAll(next.keys);
                bytes += next.bytes;
            }
            List<Request> prepared = new ArrayList<>();
            for (Request request : requests) {
                try {
                    request.prepared = request.prepare.get();
                    mutations.addAll(request.prepared.mutations);
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
                        request.prepared.committed.run();
                    } catch (Throwable t) {
                        result = t;
                    }
                }
                finish(request, result);
            }
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
