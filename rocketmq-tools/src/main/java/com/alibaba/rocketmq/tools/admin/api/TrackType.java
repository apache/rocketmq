package com.alibaba.rocketmq.tools.admin.api;

public enum TrackType {
    // 订阅了，而且消费了（Offset越过了）
    SUBSCRIBED_AND_CONSUMED,
    // 订阅了，但是被过滤掉了
    SUBSCRIBED_BUT_FILTERD,
    // 订阅了，但是是PULL，结果未知
    SUBSCRIBED_BUT_PULL,
    // 订阅了，但是没有消费（Offset小）
    SUBSCRIBED_AND_NOT_CONSUME_YET,
    // 未知异常
    UNKNOW_EXCEPTION,
}
