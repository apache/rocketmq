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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ReceiptHandleGroup {
    private final Map<String /* msgID */, Map<String /* original handle */, MessageReceiptHandle>> receiptHandleMap = new ConcurrentHashMap<>();

    public void put(String msgID, String handle, MessageReceiptHandle value) {
        Map<String, MessageReceiptHandle> handleMap = receiptHandleMap.computeIfAbsent(msgID, msgIDKey -> new ConcurrentHashMap<>());
        handleMap.put(handle, value);
    }

    public boolean isEmpty() {
        return this.receiptHandleMap.isEmpty();
    }

    public MessageReceiptHandle remove(String msgID, String handle) {
        AtomicReference<MessageReceiptHandle> resRef = new AtomicReference<>();
        receiptHandleMap.computeIfPresent(msgID, (msgIDKey, handleMap) -> {
            resRef.set(handleMap.remove(handle));
            if (handleMap.isEmpty()) {
                return null;
            }
            return handleMap;
        });
        return resRef.get();
    }

    public MessageReceiptHandle removeOne(String msgID) {
        AtomicReference<MessageReceiptHandle> resRef = new AtomicReference<>();
        receiptHandleMap.computeIfPresent(msgID, (msgIDKey, handleMap) -> {
            if (handleMap.isEmpty()) {
                return null;
            }
            Optional<String> handleKey = handleMap.keySet().stream().findAny();
            resRef.set(handleMap.remove(handleKey.get()));
            if (handleMap.isEmpty()) {
                return null;
            }
            return handleMap;
        });
        return resRef.get();
    }

    public interface DataScanner {
        void onData(String msgID, String handle, MessageReceiptHandle receiptHandle);
    }

    public void scan(DataScanner scanner) {
        this.receiptHandleMap.forEach((msgID, handleMap) -> {
            handleMap.forEach((handleStr, v) -> {
                scanner.onData(msgID, handleStr, v);
            });
        });
    }
}
