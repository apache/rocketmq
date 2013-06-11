package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 正在被消费的队列，含消息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ProcessQueue {
    private final Logger log = ClientLogger.getLog();
    private volatile boolean locked = false;
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong msgCount = new AtomicLong();


    public boolean isLocked() {
        return locked;
    }


    public void putMessage(final List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    msgTreeMap.put(msg.getQueueOffset(), msg);
                }
                msgCount.addAndGet(msgs.size());
            }
            finally {
                this.lockTreeMap.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }


    public void setLocked(boolean locked) {
        this.locked = locked;
    }


    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }
}
