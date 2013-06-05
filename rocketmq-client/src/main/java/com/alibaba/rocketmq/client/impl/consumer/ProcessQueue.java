package com.alibaba.rocketmq.client.impl.consumer;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.rocketmq.common.MessageExt;


/**
 * 正在被消费的队列，含消息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ProcessQueue {
    private volatile boolean locked = false;
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong msgCount = new AtomicLong();


    public boolean isLocked() {
        return locked;
    }


    public void setLocked(boolean locked) {
        this.locked = locked;
    }


    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }
}
