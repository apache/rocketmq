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

public class MessageReceiptHandle {
    private final String group;
    private final String topic;
    private final int queueId;
    private final String messageId;
    private final long queueOffset;
    private final String originalReceiptHandle;
    private final long timestamp;
    private final int reconsumeTimes;
    private final long expectInvisibleTime;

    private String receiptHandle;

    public MessageReceiptHandle(String group, String topic, int queueId, String receiptHandle, String messageId,
        long queueOffset, int reconsumeTimes, long expectInvisibleTime) {
        this.group = group;
        this.topic = topic;
        this.queueId = queueId;
        this.receiptHandle = receiptHandle;
        this.originalReceiptHandle = receiptHandle;
        this.messageId = messageId;
        this.queueOffset = queueOffset;
        this.reconsumeTimes = reconsumeTimes;
        this.expectInvisibleTime = expectInvisibleTime;
        this.timestamp = System.currentTimeMillis();
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public String getOriginalReceiptHandle() {
        return originalReceiptHandle;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getExpectInvisibleTime() {
        return expectInvisibleTime;
    }

    public void update(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }
}
