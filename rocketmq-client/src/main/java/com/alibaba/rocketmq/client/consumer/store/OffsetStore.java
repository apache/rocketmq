package com.alibaba.rocketmq.client.consumer.store;

import java.util.Set;

import com.alibaba.rocketmq.common.message.MessageQueue;


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
    public long readOffset(final MessageQueue mq, final boolean fromStore);


    /**
     * 持久化全部消费进度，可能持久化本地或者远端Broker
     */
    public void persistAll(final Set<MessageQueue> mqs);
}
