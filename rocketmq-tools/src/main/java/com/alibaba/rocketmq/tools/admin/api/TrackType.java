package com.alibaba.rocketmq.tools.admin.api;

public enum TrackType {
    // 订阅了，而且消费了（Offset越过了）
    CONSUMED,
    // 订阅了，但是被过滤掉了
    CONSUMED_BUT_FILTERED,
    // 订阅了，但是是PULL，结果未知
    PULL,
    // 订阅了，但是没有消费（Offset小）
    NOT_CONSUME_YET,
    // 订阅了，但是当前不在线
    NOT_ONLINE,
    // 未知异常
    UNKNOWN,
}
