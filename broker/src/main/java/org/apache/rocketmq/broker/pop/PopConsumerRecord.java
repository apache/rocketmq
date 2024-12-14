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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PopConsumerRecord {

    public enum RetryType {

        NORMAL_TOPIC(0),

        RETRY_TOPIC_V1(1),

        RETRY_TOPIC_V2(2);

        private final int code;

        RetryType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    @JSONField()
    private long popTime;

    @JSONField(ordinal = 1)
    private String groupId;

    @JSONField(ordinal = 2)
    private String topicId;

    @JSONField(ordinal = 3)
    private int queueId;

    @JSONField(ordinal = 4)
    private int retryFlag;

    @JSONField(ordinal = 5)
    private long invisibleTime;

    @JSONField(ordinal = 6)
    private long offset;

    @JSONField(ordinal = 7)
    private int attemptTimes;

    @JSONField(ordinal = 8)
    private String attemptId;

    // used for test and fastjson
    public PopConsumerRecord() {
    }

    public PopConsumerRecord(long popTime, String groupId, String topicId, int queueId,
        int retryFlag, long invisibleTime, long offset, String attemptId) {

        this.popTime = popTime;
        this.groupId = groupId;
        this.topicId = topicId;
        this.queueId = queueId;
        this.retryFlag = retryFlag;
        this.invisibleTime = invisibleTime;
        this.offset = offset;
        this.attemptId = attemptId;
    }

    @JSONField(serialize = false)
    public long getVisibilityTimeout() {
        return popTime + invisibleTime;
    }

    /**
     * Key: timestamp(8) + groupId + topicId + queueId + offset
     */
    @JSONField(serialize = false)
    public byte[] getKeyBytes() {
        int length = Long.BYTES + groupId.length() + 1 + topicId.length() + 1 + Integer.BYTES + 1 + Long.BYTES;
        byte[] bytes = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(this.getVisibilityTimeout());
        buffer.put(groupId.getBytes(StandardCharsets.UTF_8)).put((byte) '@');
        buffer.put(topicId.getBytes(StandardCharsets.UTF_8)).put((byte) '@');
        buffer.putInt(queueId).put((byte) '@');
        buffer.putLong(offset);
        return bytes;
    }

    @JSONField(serialize = false)
    public boolean isRetry() {
        return retryFlag != 0;
    }

    @JSONField(serialize = false)
    public byte[] getValueBytes() {
        return JSON.toJSONBytes(this);
    }

    public static PopConsumerRecord decode(byte[] body) {
        return JSONObject.parseObject(body, PopConsumerRecord.class);
    }

    public long getPopTime() {
        return popTime;
    }

    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getRetryFlag() {
        return retryFlag;
    }

    public void setRetryFlag(int retryFlag) {
        this.retryFlag = retryFlag;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }

    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getAttemptTimes() {
        return attemptTimes;
    }

    public void setAttemptTimes(int attemptTimes) {
        this.attemptTimes = attemptTimes;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public void setAttemptId(String attemptId) {
        this.attemptId = attemptId;
    }

    @Override
    public String toString() {
        return "PopDeliveryRecord{" +
            "popTime=" + popTime +
            ", groupId='" + groupId + '\'' +
            ", topicId='" + topicId + '\'' +
            ", queueId=" + queueId +
            ", retryFlag=" + retryFlag +
            ", invisibleTime=" + invisibleTime +
            ", offset=" + offset +
            ", attemptTimes=" + attemptTimes +
            ", attemptId='" + attemptId + '\'' +
            '}';
    }
}
