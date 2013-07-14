package com.alibaba.rocketmq.common.admin;

import java.util.HashMap;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Consumer消费进度
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-14
 */
public class ConsumerProgress extends RemotingSerializable {
    private HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<MessageQueue, OffsetWrapper>();


    public HashMap<MessageQueue, OffsetWrapper> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(HashMap<MessageQueue, OffsetWrapper> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
