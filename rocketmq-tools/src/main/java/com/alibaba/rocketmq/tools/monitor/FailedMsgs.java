package com.alibaba.rocketmq.tools.monitor;

public class FailedMsgs {
    private String consumerGroup;
    private String topic;
    private long failedMsgsTotalRecently;


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


    public long getFailedMsgsTotalRecently() {
        return failedMsgsTotalRecently;
    }


    public void setFailedMsgsTotalRecently(long failedMsgsTotalRecently) {
        this.failedMsgsTotalRecently = failedMsgsTotalRecently;
    }


    @Override
    public String toString() {
        return "FailedMsgs [consumerGroup=" + consumerGroup + ", topic=" + topic
                + ", failedMsgsTotalRecently=" + failedMsgsTotalRecently + "]";
    }
}
