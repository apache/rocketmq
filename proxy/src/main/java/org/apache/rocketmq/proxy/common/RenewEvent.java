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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;

public class RenewEvent {
    protected ReceiptHandleGroupKey key;
    protected MessageReceiptHandle messageReceiptHandle;
    protected List<MessageReceiptHandle> messageReceiptHandleList;
    protected long renewTime;
    protected List<Long> renewTimeList;
    protected EventType eventType;
    protected CompletableFuture<AckResult> future;
    protected List<CompletableFuture<AckResult>> futureList;

    public enum EventType {
        RENEW,
        STOP_RENEW,
        CLEAR_GROUP
    }

    public RenewEvent(ReceiptHandleGroupKey key, MessageReceiptHandle messageReceiptHandle, long renewTime,
        EventType eventType, CompletableFuture<AckResult> future) {
        this.key = key;
        this.messageReceiptHandle = messageReceiptHandle;
        this.messageReceiptHandleList = Collections.singletonList(messageReceiptHandle);
        this.renewTime = renewTime;
        this.renewTimeList = Collections.singletonList(renewTime);
        this.eventType = eventType;
        this.future = future;
        this.futureList = Collections.singletonList(future);
    }

    public RenewEvent(ReceiptHandleGroupKey key, List<MessageReceiptHandle> messageReceiptHandleList,
        List<Long> renewTimeList, EventType eventType, List<CompletableFuture<AckResult>> futureList) {
        this.key = key;
        this.messageReceiptHandleList = messageReceiptHandleList;
        this.messageReceiptHandle = messageReceiptHandleList == null || messageReceiptHandleList.isEmpty() ?
            null : messageReceiptHandleList.get(0);
        this.renewTimeList = renewTimeList;
        this.renewTime = renewTimeList == null || renewTimeList.isEmpty() ? 0 : renewTimeList.get(0);
        this.eventType = eventType;
        this.futureList = futureList;
        this.future = futureList == null || futureList.isEmpty() ? null : futureList.get(0);
    }

    public ReceiptHandleGroupKey getKey() {
        return key;
    }

    public MessageReceiptHandle getMessageReceiptHandle() {
        return messageReceiptHandle;
    }

    public List<MessageReceiptHandle> getMessageReceiptHandleList() {
        return messageReceiptHandleList;
    }

    public long getRenewTime() {
        return renewTime;
    }

    public List<Long> getRenewTimeList() {
        return renewTimeList;
    }

    public EventType getEventType() {
        return eventType;
    }

    public CompletableFuture<AckResult> getFuture() {
        return future;
    }

    public List<CompletableFuture<AckResult>> getFutureList() {
        return futureList;
    }
}
