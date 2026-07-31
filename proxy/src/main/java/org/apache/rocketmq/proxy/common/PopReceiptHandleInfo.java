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

/**
 * Diagnostic information for a single POP receipt handle.
 * <p>
 * Contains all relevant fields for diagnosing POP consumption issues:
 * - Message identification (group, topic, queueId, messageId, queueOffset)
 * - Consumption state (reconsumeTimes, renewTimes, renewRetryTimes, consumeTimestamp)
 * - Invisible time management (receiptHandle, nextVisibleTime, invisibleTime, isExpired)
 * - Broker location (brokerName)
 */
public class PopReceiptHandleInfo {
    private final String group;
    private final String topic;
    private final int queueId;
    private final String messageId;
    private final long queueOffset;
    private final int reconsumeTimes;
    private final int renewTimes;
    private final int renewRetryTimes;
    private final long consumeTimestamp;
    private final String receiptHandle;
    private final long nextVisibleTime;
    private final long invisibleTime;
    private final String brokerName;
    private final boolean expired;

    public PopReceiptHandleInfo(String group, String topic, int queueId, String messageId,
        long queueOffset, int reconsumeTimes, int renewTimes, int renewRetryTimes,
        long consumeTimestamp, String receiptHandle, long nextVisibleTime,
        long invisibleTime, String brokerName, boolean expired) {
        this.group = group;
        this.topic = topic;
        this.queueId = queueId;
        this.messageId = messageId;
        this.queueOffset = queueOffset;
        this.reconsumeTimes = reconsumeTimes;
        this.renewTimes = renewTimes;
        this.renewRetryTimes = renewRetryTimes;
        this.consumeTimestamp = consumeTimestamp;
        this.receiptHandle = receiptHandle;
        this.nextVisibleTime = nextVisibleTime;
        this.invisibleTime = invisibleTime;
        this.brokerName = brokerName;
        this.expired = expired;
    }

    public String getGroup() { return group; }
    public String getTopic() { return topic; }
    public int getQueueId() { return queueId; }
    public String getMessageId() { return messageId; }
    public long getQueueOffset() { return queueOffset; }
    public int getReconsumeTimes() { return reconsumeTimes; }
    public int getRenewTimes() { return renewTimes; }
    public int getRenewRetryTimes() { return renewRetryTimes; }
    public long getConsumeTimestamp() { return consumeTimestamp; }
    public String getReceiptHandle() { return receiptHandle; }
    public long getNextVisibleTime() { return nextVisibleTime; }
    public long getInvisibleTime() { return invisibleTime; }
    public String getBrokerName() { return brokerName; }
    public boolean isExpired() { return expired; }
}