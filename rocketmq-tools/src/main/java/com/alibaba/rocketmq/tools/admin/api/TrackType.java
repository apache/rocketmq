package com.alibaba.rocketmq.tools.admin.api;

public enum TrackType {
    // 已订阅，并且消息已被消费
    CONSUMED,
    // 已订阅，但消息被过滤表达式过滤
    CONSUMED_BUT_FILTERED,
    // 以 PULL 方式订阅，消费位点完全由应用方控制
    PULL,
    // 已订阅，但消息未被消费
    NOT_CONSUME_YET,
    // 已订阅，但是订阅组当前不在线
    NOT_ONLINE,
    // 未知异常，请查看 url
    UNKNOWN,
}
