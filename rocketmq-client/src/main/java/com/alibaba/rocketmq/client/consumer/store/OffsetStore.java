package com.alibaba.rocketmq.client.consumer.store;

import com.alibaba.rocketmq.common.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface OffsetStore {
    /**
     * 加载Offset
     */
    public void load();


    /**
     * 更新消费进度，存储到内存
     */
    public void updateOffset(final MessageQueue mq, final long offset);


    /**
     * 从本地缓存读取消费进度
     */
    public long readOffset(final MessageQueue mq);


    /**
     * 持久化全部消费进度，可能持久化本地或者远端Broker
     */
    public void persistAll();
}
