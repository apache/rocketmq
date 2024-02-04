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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;

public class ReceiptHandleGroup {
    protected final Map<String /* msgID */, Map<String /* original handle */, HandleData>> receiptHandleMap = new ConcurrentHashMap<>();

    public static class HandleData {
        private final Semaphore semaphore = new Semaphore(1);
        private volatile boolean needRemove = false;
        private volatile MessageReceiptHandle messageReceiptHandle;

        public HandleData(MessageReceiptHandle messageReceiptHandle) {
            this.messageReceiptHandle = messageReceiptHandle;
        }

        public boolean lock(long timeoutMs) {
            try {
                return this.semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }

        public void unlock() {
            this.semaphore.release();
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

    public void put(String msgID, String handle, MessageReceiptHandle value) {
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        Map<String, HandleData> handleMap = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Map<String, HandleData>>) this.receiptHandleMap,
            msgID, msgIDKey -> new ConcurrentHashMap<>());
        handleMap.compute(handle, (handleKey, handleData) -> {
            if (handleData == null || handleData.needRemove) {
                return new HandleData(value);
            }
            if (!handleData.lock(timeout)) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to put handle failed");
            }
            try {
                if (handleData.needRemove) {
                    return new HandleData(value);
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

    public MessageReceiptHandle get(String msgID, String handle) {
        Map<String, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        handleMap.computeIfPresent(handle, (handleKey, handleData) -> {
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
        Map<String, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return null;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        AtomicReference<MessageReceiptHandle> res = new AtomicReference<>();
        handleMap.computeIfPresent(handle, (handleKey, handleData) -> {
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

    public void computeIfPresent(String msgID, String handle,
        Function<MessageReceiptHandle, CompletableFuture<MessageReceiptHandle>> function) {
        Map<String, HandleData> handleMap = this.receiptHandleMap.get(msgID);
        if (handleMap == null) {
            return;
        }
        long timeout = ConfigurationManager.getProxyConfig().getLockTimeoutMsInHandleGroup();
        handleMap.computeIfPresent(handle, (handleKey, handleData) -> {
            if (!handleData.lock(timeout)) {
                throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "try to compute failed");
            }
            CompletableFuture<MessageReceiptHandle> future = function.apply(handleData.messageReceiptHandle);
            future.whenComplete((messageReceiptHandle, throwable) -> {
                try {
                    if (throwable != null) {
                        return;
                    }
                    if (messageReceiptHandle == null) {
                        handleData.needRemove = true;
                    } else {
                        handleData.messageReceiptHandle = messageReceiptHandle;
                    }
                } finally {
                    handleData.unlock();
                }
                if (handleData.needRemove) {
                    handleMap.remove(handleKey, handleData);
                }
                removeHandleMapKeyIfNeed(msgID);
            });
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
            handleMap.forEach((handleStr, v) -> {
                scanner.onData(msgID, handleStr, v.messageReceiptHandle);
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
