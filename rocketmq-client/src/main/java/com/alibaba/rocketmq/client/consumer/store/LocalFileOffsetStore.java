package com.alibaba.rocketmq.client.consumer.store;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 消费进度存储到Consumer本地
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class LocalFileOffsetStore implements OffsetStore {
    private final MQClientFactory mQClientFactory;
    private final String groupName;
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();


    public LocalFileOffsetStore(MQClientFactory mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }


    @Override
    public void load() {
    }


    @Override
    public void updateOffset(MessageQueue mq, long offset) {
    }


    @Override
    public long readOffset(MessageQueue mq, boolean fromStore) {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        // TODO Auto-generated method stub

    }
}
