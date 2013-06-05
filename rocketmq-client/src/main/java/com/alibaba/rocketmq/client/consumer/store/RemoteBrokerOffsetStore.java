package com.alibaba.rocketmq.client.consumer.store;

import com.alibaba.rocketmq.common.MessageQueue;


/**
 * 消费进度存储到远端Broker，比较可靠
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    @Override
    public void load() {
        // TODO Auto-generated method stub

    }


    @Override
    public void updateOffset(MessageQueue mq, long offset) {
        // TODO Auto-generated method stub

    }


    @Override
    public long readOffset(MessageQueue mq) {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public void persistAll() {
        // TODO Auto-generated method stub

    }
}
