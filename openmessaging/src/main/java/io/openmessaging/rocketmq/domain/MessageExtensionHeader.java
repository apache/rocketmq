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
package io.openmessaging.rocketmq.domain;

import io.openmessaging.extension.ExtensionHeader;

public class MessageExtensionHeader implements ExtensionHeader {
    private int partition;

    private long offset;

    private String correlationId;

    private String transactionId;

    private long storeTimestamp;

    private String storeHost;

    private String messageKey;

    private String traceId;

    private long delayTime;

    private long expireTime;

    @Override public ExtensionHeader setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    @Override public ExtensionHeader setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    @Override public ExtensionHeader setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    @Override public ExtensionHeader setTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    @Override public ExtensionHeader setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
        return this;
    }

    @Override public ExtensionHeader setStoreHost(String storeHost) {
        this.storeHost = storeHost;
        return this;
    }

    @Override public ExtensionHeader setMessageKey(String messageKey) {
        this.messageKey = messageKey;
        return this;
    }

    @Override public ExtensionHeader setTraceId(String traceId) {
        this.traceId = traceId;
        return this;
    }

    @Override public ExtensionHeader setDelayTime(long delayTime) {
        this.delayTime = delayTime;
        return this;
    }

    @Override public ExtensionHeader setExpireTime(long expireTime) {
        this.expireTime = expireTime;
        return this;
    }

    @Override public int getPartiton() {
        return partition;
    }

    @Override public long getOffset() {
        return offset;
    }

    @Override public String getCorrelationId() {
        return correlationId;
    }

    @Override public String getTransactionId() {
        return transactionId;
    }

    @Override public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override public String getStoreHost() {
        return storeHost;
    }

    @Override public long getDelayTime() {
        return delayTime;
    }

    @Override public long getExpireTime() {
        return expireTime;
    }

    @Override public String getMessageKey() {
        return messageKey;
    }

    @Override public String getTraceId() {
        return traceId;
    }

    @Override public String toString() {
        return "MessageExtensionHeader{" +
            "partition=" + partition +
            ", offset=" + offset +
            ", correlationId='" + correlationId + '\'' +
            ", transactionId='" + transactionId + '\'' +
            ", storeTimestamp=" + storeTimestamp +
            ", storeHost='" + storeHost + '\'' +
            ", messageKey='" + messageKey + '\'' +
            ", traceId='" + traceId + '\'' +
            ", delayTime=" + delayTime +
            ", expireTime=" + expireTime +
            '}';
    }
}
