package com.alibaba.rocketmq.client.impl.consumer;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 严格保证单个队列同一时刻只有一个线程消费
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-25
 */
public class MessageQueueLock {
    private ConcurrentHashMap<MessageQueue, Object> mqLockTable =
            new ConcurrentHashMap<MessageQueue, Object>();


    public Object fetchLockObject(final MessageQueue mq) {
        Object objLock = this.mqLockTable.get(mq);
        if (null == objLock) {
            objLock = new Object();
            Object prevLock = this.mqLockTable.putIfAbsent(mq, objLock);
            if (prevLock != null) {
                objLock = prevLock;
            }
        }

        return objLock;
    }
}
