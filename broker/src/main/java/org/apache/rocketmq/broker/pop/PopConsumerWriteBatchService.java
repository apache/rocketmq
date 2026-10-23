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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;

/**
 * One WAL writer for all POP KV mutations. Admission never waits for disk or queue space.
 * Requests retain their order and are never split across WriteBatches.
 */
class PopConsumerWriteBatchService {
    private static final int MAX_PENDING_REQUESTS = 4096;
    private static final long MAX_PENDING_BYTES = 64 * 1024 * 1024;
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

    private static class Request {
        final List<Mutation> mutations;
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final long bytes;
        final boolean asyncCompletion;

        Request(List<Mutation> mutations, boolean asyncCompletion) {
            this.mutations = mutations;
            this.asyncCompletion = asyncCompletion;
            this.bytes = mutations.stream().mapToLong(m -> m.key.length + (m.value == null ? 0L : m.value.length)).sum();
        }
    }

    private final Object admissionLock = new Object();
    private final LinkedBlockingQueue<Request> queue = new LinkedBlockingQueue<>(MAX_PENDING_REQUESTS);
    private final BatchWriter writer;
    // Bounded indirectly by admission, including requests waiting for their completion callbacks.
    private final ExecutorService completions = Executors.newSingleThreadExecutor(
        new ThreadFactoryImpl("PopKVCompletion_"));
    private final Thread thread = new Thread(this::run, "PopKVGroupCommit");
    private volatile boolean accepting;
    private int pendingRequests;
    private long pendingBytes;

    PopConsumerWriteBatchService(BatchWriter writer) {
        this.writer = writer;
    }

    void start() {
        synchronized (admissionLock) {
            accepting = true;
            thread.start();
        }
    }

    CompletableFuture<Void> submit(List<Mutation> mutations) {
        return submit(mutations, true);
    }

    // Used only by blocking compatibility methods; their private futures have no continuations.
    CompletableFuture<Void> submit(List<Mutation> mutations, boolean asyncCompletion) {
        Request request = new Request(mutations, asyncCompletion);
        synchronized (admissionLock) {
            if (!accepting || pendingRequests >= MAX_PENDING_REQUESTS || pendingBytes + request.bytes > MAX_PENDING_BYTES) {
                request.future.completeExceptionally(new RejectedExecutionException("POP KV writer stopped or full"));
                return request.future;
            }
            pendingRequests++;
            pendingBytes += request.bytes;
            queue.add(request);
        }
        return request.future;
    }

    private void run() {
        while (accepting || !queue.isEmpty()) {
            Request first;
            try {
                first = queue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Shutdown never interrupts an in-progress native write.
                continue;
            }
            if (first == null) {
                continue;
            }
            List<Request> requests = new ArrayList<>();
            List<Mutation> mutations = new ArrayList<>();
            requests.add(first);
            mutations.addAll(first.mutations);
            long bytes = first.bytes;
            while (requests.size() < MAX_BATCH_REQUESTS) {
                Request next = queue.peek();
                if (next == null || bytes + next.bytes > MAX_BATCH_BYTES) {
                    break;
                }
                queue.poll();
                requests.add(next);
                mutations.addAll(next.mutations);
                bytes += next.bytes;
            }
            Throwable failure = null;
            try {
                writer.write(mutations);
            } catch (Throwable t) {
                failure = t;
            }
            final Throwable result = failure;
            List<Request> asyncRequests = new ArrayList<>();
            for (Request request : requests) {
                if (request.asyncCompletion) {
                    asyncRequests.add(request);
                } else {
                    complete(request, result);
                }
            }
            // Never execute user continuations on the WAL writer. Private blocking futures
            // above are completed directly so a callback can safely use a synchronous API.
            if (!asyncRequests.isEmpty()) {
                completions.execute(() -> asyncRequests.forEach(request -> complete(request, result)));
            }
        }
    }

    private void complete(Request request, Throwable failure) {
        synchronized (admissionLock) {
            pendingRequests--;
            pendingBytes -= request.bytes;
        }
        if (failure == null) {
            request.future.complete(null);
        } else {
            request.future.completeExceptionally(failure);
        }
    }

    void shutdown() {
        synchronized (admissionLock) {
            accepting = false;
        }
        boolean interrupted = false;
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
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
