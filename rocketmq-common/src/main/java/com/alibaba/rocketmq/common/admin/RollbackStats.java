package com.alibaba.rocketmq.common.admin;

/**
 * 按时间回溯消费进度
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-9-12
 */
public class RollbackStats {
    private String brokerName;
    private long queueId;
    private long brokerOffset;
    private long consumerOffset;
    private long timestampOffset;
    private long rollbackOffset;


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public long getQueueId() {
        return queueId;
    }


    public void setQueueId(long queueId) {
        this.queueId = queueId;
    }


    public long getBrokerOffset() {
        return brokerOffset;
    }


    public void setBrokerOffset(long brokerOffset) {
        this.brokerOffset = brokerOffset;
    }


    public long getConsumerOffset() {
        return consumerOffset;
    }


    public void setConsumerOffset(long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }


    public long getTimestampOffset() {
        return timestampOffset;
    }


    public void setTimestampOffset(long timestampOffset) {
        this.timestampOffset = timestampOffset;
    }


    public long getRollbackOffset() {
        return rollbackOffset;
    }


    public void setRollbackOffset(long rollbackOffset) {
        this.rollbackOffset = rollbackOffset;
    }
}
