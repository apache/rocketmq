package com.alibaba.rocketmq.common.filter;

public class FilterContext {
    private String consumerGroup;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}
