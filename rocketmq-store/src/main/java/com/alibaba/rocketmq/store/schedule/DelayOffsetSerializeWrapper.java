package com.alibaba.rocketmq.store.schedule;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


public class DelayOffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentHashMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);


    public ConcurrentHashMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(ConcurrentHashMap<Integer, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
