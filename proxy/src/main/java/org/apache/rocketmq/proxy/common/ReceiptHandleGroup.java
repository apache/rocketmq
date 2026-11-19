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

package org.apache.rocketmq.proxy.common;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public class ReceiptHandleGroup {
    private static final int LOCK_EXPIRATION_MULTIPLIER = 3;
    private final LongSupplier nanoTime;

    // The messages having the same messageId will be deduplicated based on the parameters of broker, queueId, and offset
    protected final Map<String /* msgID */, Map<HandleKey, HandleData>> receiptHandleMap = new ConcurrentHashMap<>();

    public ReceiptHandleGroup() {
        this(System::nanoTime);
    }

    ReceiptHandleGroup(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    public static class HandleKey {
        private final String originalHandle;
        private final String broker;
        private final int queueId;
        private final long offset;

        public HandleKey(String handle) {
            this(ReceiptHandle.decode(handle));
        }

        public HandleKey(ReceiptHandle receiptHandle) {
            this.originalHandle = receiptHandle.getReceiptHandle();
            this.broker = receiptHandle.getBrokerName();
            this.queueId = receiptHandle.getQueueId();
            this.offset = receiptHandle.getOffset();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            HandleKey key = (HandleKey) o;
            return queueId == key.queueId && offset == key.offset && Objects.equal(broker, key.broker);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(broker, queueId, offset);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                .append("originalHandle", originalHandle)
                .append("broker", broker)
                .append("queueId", queueId)
                .append("offset", offset)
                .toString();
        }

        public String getOriginalHandle() {
            return originalHandle;
        }

        public String getBroker() {
            return broker;
        }

        public int getQueueId() {
            return queueId;
        }

        public long getOffset() {
            return offset;
        }
    }

    private static final class PendingComputation {
        private final long startTimeNanos;

        PendingComputation(long startTimeNanos) {
            this.startTimeNanos = startTimeNanos;
        }
    }

    public static class HandleData {
        private final Semaphore semaphore = new Semaphore(1);
        private final LongSupplier nanoTime;
        // Guarded by this. Only a computation has an owner that can be invalidated.
        private PendingComputation pendingComputation;
        private volatile boolean needRemove = false;
        private volatile MessageReceiptHandle messageReceiptHandle;

        private HandleData(MessageReceiptHandle messageReceiptHandle, LongSupplier nanoTime) {
            this.messageReceiptHandle = messageReceiptHandle;
            this.nanoTime = nanoTime;
        }

        private boolean lock(long timeoutMs) {
            try {
                if (this.semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                    return true;
                }
                // Use the configured timeout for expiry even when renewal passes zero to avoid waiting.
                return reclaimExpiredComputation() && this.semaphore.tryAcquire(0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void unlock() {
            this.semaphore.release();
        }

        private synchronized boolean reclaimExpiredComputation() {
            if (pendingComputation == null) {
                return false;
            }
            long elapsedNanos = nanoTime.getAsLong() - pendingComputation.startTimeNanos;
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup());
            if (elapsedNanos / LOCK_EXPIRATION_MULTIPLIER <= timeoutNanos) {
                return false;
            }
            // Completion checks the same token under this monitor before publishing or releasing.
            pendingComputation = null;
            unlock();
            return true;
        }

        private synchronized PendingComputation beginComputation() {
            pendingComputation = new PendingComputation(nanoTime.getAsLong());
            return pendingComputation;
        }

        private synchronized boolean completeComputation(PendingComputation owner,
            MessageReceiptHandle result, Throwable error) {
            if (pendingComputation != owner) {
                return false;
            }
            try {
                if (error != null) {
                    return false;
                }
                if (result == null) {
                    needRemove = true;
                    return true;
                }
                messageReceiptHandle = result;
                return false;
            } finally {
                pendingComputation = null;
                unlock();
            }
        }

        public MessageReceiptHandle getMessageReceiptHandle() {
            return messageReceiptHandle;
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(semaphore, needRemove, messageReceiptHandle);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("semaphore", semaphore)
                .add("needRemove", needRemove)
                .add("messageReceiptHandle", messageReceiptHandle)
                .toString();
        }
    }

    public void put(String msgID, MessageReceiptHandle value) {
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        Map<HandleKey, HandleData> handleMap = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Map<HandleKey, HandleData>>) this.receiptHandleMap,
            msgID, msgIDKey -> new ConcurrentHashMap<>());
        handleMap.compute(new HandleKey(value.getOriginalReceiptHandle()), (handleKey, handleData) -> {
            if (handleData == null || handleData.needRemove) {
                return new HandleData(value, nanoTime);
            }
            if (!handleData.lock(timeout)) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to put handle failed");
            }
            try {
                if (handleData.needRemove) {
                    return new HandleData(value, nanoTime);
                }
                handleData.messageReceiptHandle = value;
            } finally {
                handleData.unlock();
            }
            return handleData;
        });
    }

    public boolean isEmpty() {
        return this.receiptHandleMap.isEmpty();
    }

    public long getHandleNum() {
        long handleNum = 0L;
        for (Map.Entry<String, Map<HandleKey, HandleData>> entry : receiptHandleMap.entrySet()) {
            handleNum += entry.getValue().size();
        }
        return handleNum;
    }

    public int getMsgCount() {
        return this.receiptHandleMap.size();
    }

    public MessageReceiptHandle get(String msgID, String handle) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        handleMap.computeIfPresent(new HandleKey(handle), (handleKey, handleData) -> {
            if (!handleData.lock(timeout)) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to get handle failed");
            }
            try {
                if (handleData.needRemove) {
                    return null;
                }
                res.set(handleData.messageReceiptHandle);
            } finally {
                handleData.unlock();
            }
            return handleData;
        });
        return res.get();
    }

    public MessageReceiptHandle remove(String msgID, String handle) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        handleMap.computeIfPresent(new HandleKey(handle), (handleKey, handleData) -> {
            if (!handleData.lock(timeout)) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to remove and get handle failed");
            }
            try {
                if (!handleData.needRemove) {
                    handleData.needRemove = true;
                    res.set(handleData.messageReceiptHandle);
                }
                return null;
            } finally {
                handleData.unlock();
            }
        });
        removeHandleMapKeyIfNeed(msgID);
        return res.get();
    }

    public MessageReceiptHandle removeOne(String msgID) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        Set<HandleKey> keys = handleMap.keySet();
        for (HandleKey key : keys) {
            MessageReceiptHandle res = this.remove(msgID, key.originalHandle);
            if (res != null) {
                return res;
            }
        }
        return null;
    }

    /**
     * Computes on a detached handle and publishes the result only while the computation owns the permit.
     */
    public void computeIfPresent(String msgID, String handle,
        Function<MessageReceiptHandle, CompletableFuture<MessageReceiptHandle>> function) {
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        computeIfPresent(msgID, handle, function, timeout);
    }

    public void computeIfPresent(String msgID, String handle,
        Function<MessageReceiptHandle, CompletableFuture<MessageReceiptHandle>> function, long lockTimeout) {
        Map<HandleKey, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return;
        }
        handleMap.computeIfPresent(new HandleKey(handle), (handleKey, handleData) -> {
            if (!handleData.lock(lockTimeout)) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to compute failed");
            }
            if (handleData.needRemove) {
                // A completed removal can still be waiting to unlink this entry from the map.
                handleData.unlock();
                return null;
            }
            CompletableFuture<MessageReceiptHandle> future;
            try {
                // A late renewal can mutate its copy, but not the live handle after ownership changes.
                future = function.apply(new MessageReceiptHandle(handleData.messageReceiptHandle));
            } catch (RuntimeException | Error e) {
                handleData.unlock();
                throw e;
            }
            // Only publish a reclaimable owner once the synchronous part of the computation has returned.
            PendingComputation owner = handleData.beginComputation();
            try {
                future.whenComplete((result, error) -> {
                    if (handleData.completeComputation(owner, result, error)) {
                        handleMap.remove(handleKey, handleData);
                        removeHandleMapKeyIfNeed(msgID);
                    }
                });
            } catch (RuntimeException | Error e) {
                // Registration can fail before the completion handler has taken responsibility for the permit.
                handleData.completeComputation(owner, null, e);
                throw e;
            }
            return handleData;
        });
    }

    protected void removeHandleMapKeyIfNeed(String msgID) {
        this.receiptHandleMap.computeIfPresent(msgID, (msgIDKey, handleMap) -> {
            if (handleMap.isEmpty()) {
                return null;
            }
            return handleMap;
        });
    }

    public interface DataScanner {
        void onData(String msgID, String handle, MessageReceiptHandle receiptHandle);
    }

    public void scan(DataScanner scanner) {
        this.receiptHandleMap.forEach((msgID, handleMap) -> {
            handleMap.forEach((handleKey, v) -> {
                scanner.onData(msgID, handleKey.originalHandle, v.messageReceiptHandle);
            });
        });
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("receiptHandleMap", receiptHandleMap)
            .toString();
    }
}
