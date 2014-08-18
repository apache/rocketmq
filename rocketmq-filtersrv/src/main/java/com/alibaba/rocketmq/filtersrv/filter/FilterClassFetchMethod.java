package com.alibaba.rocketmq.filtersrv.filter;

public interface FilterClassFetchMethod {
    public String fetch(final String topic, final String consumerGroup, final String className);
}
