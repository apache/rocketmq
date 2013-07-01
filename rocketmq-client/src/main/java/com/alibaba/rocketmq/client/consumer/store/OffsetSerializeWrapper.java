package com.alibaba.rocketmq.client.consumer.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


public class OffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();


    public ConcurrentHashMap<MessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }
}