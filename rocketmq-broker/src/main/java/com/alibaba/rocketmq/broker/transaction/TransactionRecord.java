package com.alibaba.rocketmq.broker.transaction;

public class TransactionRecord {
    // Commit Log Offset
    private long offset;
    // 消息写入时间戳
    private long storeTimestamp;
    private String producerGroup;


    public long getOffset() {
        return offset;
    }


    public void setOffset(long offset) {
        this.offset = offset;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
