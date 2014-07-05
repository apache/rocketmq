package com.alibaba.rocketmq.tools.monitor;

public class UndoneMsgs {
    private String consumerGroup;
    private String topic;
    // 堆积的消息总数
    private long undoneMsgsTotal;
    // 单个队列堆积的最多消息数
    private long undoneMsgsSingleMQ;
    // Delay的最长时间，单位秒
    private long undoneMsgsDelaySeconds;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public long getUndoneMsgsTotal() {
        return undoneMsgsTotal;
    }


    public void setUndoneMsgsTotal(long undoneMsgsTotal) {
        this.undoneMsgsTotal = undoneMsgsTotal;
    }


    public long getUndoneMsgsSingleMQ() {
        return undoneMsgsSingleMQ;
    }


    public void setUndoneMsgsSingleMQ(long undoneMsgsSingleMQ) {
        this.undoneMsgsSingleMQ = undoneMsgsSingleMQ;
    }


    public long getUndoneMsgsDelaySeconds() {
        return undoneMsgsDelaySeconds;
    }


    public void setUndoneMsgsDelaySeconds(long undoneMsgsDelaySeconds) {
        this.undoneMsgsDelaySeconds = undoneMsgsDelaySeconds;
    }
}
