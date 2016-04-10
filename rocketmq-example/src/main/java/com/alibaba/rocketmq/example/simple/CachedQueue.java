package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.TreeMap;


public class CachedQueue {
    private final TreeMap<Long, MessageExt> msgCachedTable = new TreeMap<Long, MessageExt>();


    public TreeMap<Long, MessageExt> getMsgCachedTable() {
        return msgCachedTable;
    }
}
