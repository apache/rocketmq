/**
 * $Id: DispatchRequest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

/**
 * 分发消息位置信息到逻辑队列和索引服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
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
    private final long tranStateTableOffset;
    private final long preparedTransactionOffset;
    private final String producerGroup;


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
            final long tranStateTableOffset,// 10
            final long preparedTransactionOffset,// 11
            final String producerGroup// 12
                                      // 如果producerGroup为空，表示是recover过程，所以不更新
                                      // Transaction state
                                      // table
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
        this.tranStateTableOffset = tranStateTableOffset;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.producerGroup = producerGroup;
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
        this.tranStateTableOffset = 0;
        this.preparedTransactionOffset = 0;
        this.producerGroup = "";
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


    public long getTranStateTableOffset() {
        return tranStateTableOffset;
    }


    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }


    public String getProducerGroup() {
        return producerGroup;
    }
}
