package com.alibaba.rocketmq.client.consumer.store;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 消费进度存储到Consumer本地，不是很可靠
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class LocalFileOffsetStore implements OffsetStore {
    @Override
    public void load() {
    }


    @Override
    public void updateOffset(MessageQueue mq, long offset) {
    }


    @Override
    public long readOffset(MessageQueue mq) {
        return 0;
    }


    @Override
    public void persistAll() {
    }
}
