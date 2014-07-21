package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 查看客户端消费组的消费情况。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-12-30
 */
@Deprecated
public class GetConsumerStatusBody extends RemotingSerializable {
    private Map<MessageQueue, Long> messageQueueTable = new HashMap<MessageQueue, Long>();
    private Map<String, Map<MessageQueue, Long>> consumerTable =
            new HashMap<String, Map<MessageQueue, Long>>();


    public Map<MessageQueue, Long> getMessageQueueTable() {
        return messageQueueTable;
    }


    public void setMessageQueueTable(Map<MessageQueue, Long> messageQueueTable) {
        this.messageQueueTable = messageQueueTable;
    }


    public Map<String, Map<MessageQueue, Long>> getConsumerTable() {
        return consumerTable;
    }


    public void setConsumerTable(Map<String, Map<MessageQueue, Long>> consumerTable) {
        this.consumerTable = consumerTable;
    }
}
