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
package org.apache.rocketmq.store;

import java.util.Map;

/**
 * 分发请求
 */
public class DispatchRequest {
    //消息主题
    private final String topic;
    //队列ID
    private final int queueId;
    //消息物理偏移量
    private final long commitLogOffset;
    //消息长度
    private final int msgSize;
    //消息过滤tag hashcode
    private final long tagsCode;
    //消息存储时间戳
    private final long storeTimestamp;
    //消费队列偏移量
    private final long consumeQueueOffset;
    //消息索引，多个用空格隔开
    private final String keys;
    //是否成功解析到完整的消息
    private final boolean success;
    //消息唯一键
    private final String uniqKey;
    //消息系统标记
    private final int sysFlag;
    //消息预处理事务偏移量
    private final long preparedTransactionOffset;
    //消息属性
    private final Map<String, String> propertiesMap;
    //位图
    private byte[] bitMap;

    public DispatchRequest(
        final String topic,
        final int queueId,
        final long commitLogOffset,
        final int msgSize,
        final long tagsCode,
        final long storeTimestamp,
        final long consumeQueueOffset,
        final String keys,
        final String uniqKey,
        final int sysFlag,
        final long preparedTransactionOffset,
        final Map<String, String> propertiesMap
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }
}
