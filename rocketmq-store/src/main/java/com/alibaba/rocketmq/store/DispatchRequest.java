/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store;

/**
 * 分发消息位置信息到逻辑队列和索引服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class DispatchRequest {
    private final String topic;
    private final int queueId;
    private final long commitLogOffset;
    private final int msgSize;
    private final long tagsCode;
    private final long storeTimestamp;
    private final long consumeQueueOffset;
    private final String keys;
    /**
     * 事务相关部分
     */
    private final int sysFlag;
    private final long preparedTransactionOffset;


    public DispatchRequest(//
            final String topic,// 1
            final int queueId,// 2
            final long commitLogOffset,// 3
            final int msgSize,// 4
            final long tagsCode,// 5
            final long storeTimestamp,// 6
            final long consumeQueueOffset,// 7
            final String keys,// 8
            /**
             * 事务相关部分
             */
            final int sysFlag,// 9
            final long preparedTransactionOffset// 10
    ) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;

        /**
         * 事务相关部分
         */
        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
    }


    public DispatchRequest(int size) {
        // 1
        this.topic = "";
        // 2
        this.queueId = 0;
        // 3
        this.commitLogOffset = 0;
        // 4
        this.msgSize = size;
        // 5
        this.tagsCode = 0;
        // 6
        this.storeTimestamp = 0;
        // 7
        this.consumeQueueOffset = 0;
        // 8
        this.keys = "";

        /**
         * 事务相关部分
         */
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
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
}
