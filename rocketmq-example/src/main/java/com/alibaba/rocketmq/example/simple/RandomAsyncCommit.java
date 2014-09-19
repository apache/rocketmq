package com.alibaba.rocketmq.example.simple;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


public class RandomAsyncCommit {
    private final ConcurrentHashMap<MessageQueue, CachedQueue> mqCachedTable =
            new ConcurrentHashMap<MessageQueue, CachedQueue>();


    public void putMessages(final MessageQueue mq, final List<MessageExt> msgs) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null == cachedQueue) {
            cachedQueue = new CachedQueue();
            this.mqCachedTable.put(mq, cachedQueue);
        }
        for (MessageExt msg : msgs) {
            cachedQueue.getMsgCachedTable().put(msg.getQueueOffset(), msg);
        }
    }


    public void removeMessage(final MessageQueue mq, long offset) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null != cachedQueue) {
            cachedQueue.getMsgCachedTable().remove(offset);
        }
    }


    /**
     * 可以被提交的Offset
     */
    public long commitableOffset(final MessageQueue mq) {
        CachedQueue cachedQueue = this.mqCachedTable.get(mq);
        if (null != cachedQueue) {
            return cachedQueue.getMsgCachedTable().firstKey();
        }

        return -1;
    }
}
