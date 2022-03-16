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

package org.apache.rocketmq.common.consumer;

import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;

public class ReceiptHandle {
    private static final String SEPARATOR = MessageConst.KEY_SEPARATOR;
    private static final String NORMAL_TOPIC = "0";
    private static final String RETRY_TOPIC = "1";
    private final long startOffset;
    private final long retrieveTime;
    private final long invisibleTime;
    private final long nextVisibleTime;
    private final int reviveQueueId;
    private final String topic;
    private final String brokerName;
    private final int queueId;
    private final long offset;
    private final String receiptHandle;

    public String encode() {
        String t = NORMAL_TOPIC;
        if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            t = RETRY_TOPIC;
        }
        return startOffset + SEPARATOR + retrieveTime + SEPARATOR + invisibleTime + SEPARATOR + reviveQueueId + SEPARATOR + t + SEPARATOR + brokerName + SEPARATOR + queueId + SEPARATOR + offset;
    }

    public boolean isExpired() {
        return nextVisibleTime <= System.currentTimeMillis();
    }

    public static ReceiptHandle decode(String receiptHandle) {
        List<String> dataList = Arrays.asList(receiptHandle.split(SEPARATOR));
        if (dataList.size() < 8) {
            throw new IllegalArgumentException("Parse failed, dataList size " + dataList.size());
        }
        long startOffset = Long.parseLong(dataList.get(0));
        long retrieveTime = Long.parseLong(dataList.get(1));
        long invisibleTime = Long.parseLong(dataList.get(2));
        int reviveQueueId = Integer.parseInt(dataList.get(3));
        String topic = dataList.get(4);
        String brokerName = dataList.get(5);
        int queueId = Integer.parseInt(dataList.get(6));
        long offset = Long.parseLong(dataList.get(7));
        return new ReceiptHandleBuilder()
            .startOffset(startOffset)
            .retrieveTime(retrieveTime)
            .invisibleTime(invisibleTime)
            .reviveQueueId(reviveQueueId)
            .topic(topic)
            .brokerName(brokerName)
            .queueId(queueId)
            .offset(offset)
            .nextVisibleTime(retrieveTime + invisibleTime)
            .receiptHandle(receiptHandle).build();
    }

    ReceiptHandle(final long startOffset, final long retrieveTime, final long invisibleTime, final long nextVisibleTime,
        final int reviveQueueId, final String topic, final String brokerName, final int queueId, final long offset,
        final String receiptHandle) {
        this.startOffset = startOffset;
        this.retrieveTime = retrieveTime;
        this.invisibleTime = invisibleTime;
        this.nextVisibleTime = nextVisibleTime;
        this.reviveQueueId = reviveQueueId;
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
        this.offset = offset;
        this.receiptHandle = receiptHandle;
    }

    public static class ReceiptHandleBuilder {
        private long startOffset;
        private long retrieveTime;
        private long invisibleTime;
        private long nextVisibleTime;
        private int reviveQueueId;
        private String topic;
        private String brokerName;
        private int queueId;
        private long offset;
        private long commitLogOffset;
        private String type;
        private String receiptHandle;

        ReceiptHandleBuilder() {
        }

        public ReceiptHandle.ReceiptHandleBuilder startOffset(final long startOffset) {
            this.startOffset = startOffset;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder retrieveTime(final long retrieveTime) {
            this.retrieveTime = retrieveTime;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder invisibleTime(final long invisibleTime) {
            this.invisibleTime = invisibleTime;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder nextVisibleTime(final long nextVisibleTime) {
            this.nextVisibleTime = nextVisibleTime;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder reviveQueueId(final int reviveQueueId) {
            this.reviveQueueId = reviveQueueId;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder topic(final String topic) {
            this.topic = topic;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder brokerName(final String brokerName) {
            this.brokerName = brokerName;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder queueId(final int queueId) {
            this.queueId = queueId;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder offset(final long offset) {
            this.offset = offset;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder commitLogOffset(final long commitLogOffset) {
            this.commitLogOffset = commitLogOffset;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder type(final String type) {
            this.type = type;
            return this;
        }

        public ReceiptHandle.ReceiptHandleBuilder receiptHandle(final String receiptHandle) {
            this.receiptHandle = receiptHandle;
            return this;
        }

        public ReceiptHandle build() {
            return new ReceiptHandle(this.startOffset, this.retrieveTime, this.invisibleTime, this.nextVisibleTime, this.reviveQueueId, this.topic, this.brokerName, this.queueId, this.offset, this.receiptHandle);
        }

        @java.lang.Override
        public java.lang.String toString() {
            return "ReceiptHandle.ReceiptHandleBuilder(startOffset=" + this.startOffset + ", retrieveTime=" + this.retrieveTime + ", invisibleTime=" + this.invisibleTime + ", nextVisibleTime=" + this.nextVisibleTime + ", reviveQueueId=" + this.reviveQueueId + ", topic=" + this.topic + ", brokerName=" + this.brokerName + ", queueId=" + this.queueId + ", offset=" + this.offset + ", commitLogOffset=" + this.commitLogOffset + ", type=" + this.type + ", receiptHandle=" + this.receiptHandle + ")";
        }
    }

    public static ReceiptHandle.ReceiptHandleBuilder builder() {
        return new ReceiptHandle.ReceiptHandleBuilder();
    }

    public long getStartOffset() {
        return this.startOffset;
    }

    public long getRetrieveTime() {
        return this.retrieveTime;
    }

    public long getInvisibleTime() {
        return this.invisibleTime;
    }

    public long getNextVisibleTime() {
        return this.nextVisibleTime;
    }

    public int getReviveQueueId() {
        return this.reviveQueueId;
    }

    public String getTopic() {
        return this.topic;
    }

    public String getBrokerName() {
        return this.brokerName;
    }

    public int getQueueId() {
        return this.queueId;
    }

    public long getOffset() {
        return this.offset;
    }

    public String getReceiptHandle() {
        return this.receiptHandle;
    }
}
