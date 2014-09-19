package com.alibaba.rocketmq.example.simple;

import java.util.TreeMap;

import com.alibaba.rocketmq.common.message.MessageExt;


public class CachedQueue {
    private final TreeMap<Long, MessageExt> msgCachedTable = new TreeMap<Long, MessageExt>();


    public TreeMap<Long, MessageExt> getMsgCachedTable() {
        return msgCachedTable;
    }
}
