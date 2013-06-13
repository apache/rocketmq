/**
 * $Id: AllocateMessageQueueByConfig.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer.rebalance;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * 按照配置来分配队列，建议应用使用Spring来初始化
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {
    private List<MessageQueue> messageQueueList;


    @Override
    public List<MessageQueue> allocate(String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        return this.messageQueueList;
    }


    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }


    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }
}
