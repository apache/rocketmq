package com.alibaba.rocketmq.store;

/**
 * Created by manhong.yqd<jodie.yqd@gmail.com> on 15/6/19.
 */
public interface MessageArrivingListener {
    void arriving(String topic, int queueId, long logicOffset, long tagsCode);
}
