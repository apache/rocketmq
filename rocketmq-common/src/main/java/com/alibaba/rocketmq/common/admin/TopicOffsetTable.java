package com.alibaba.rocketmq.common.admin;

import java.util.HashMap;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * Topic所有队列的Offset
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class TopicOffsetTable {
    private HashMap<MessageQueue, TopicOffset> offsetTable = new HashMap<MessageQueue, TopicOffset>();


    public HashMap<MessageQueue, TopicOffset> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(HashMap<MessageQueue, TopicOffset> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
