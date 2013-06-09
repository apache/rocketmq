/**
 * $Id: AllocateMessageQueueStrategy.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * Consumer队列自动分配策略
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public interface AllocateMessageQueueStrategy {
    /**
     * 队列分配算法
     * 
     * @param group
     * @param topic
     * @param currentCID
     *            当前ConsumerId
     * @param mqAll
     *            当前Topic的所有队列集合，无重复数据，且有序
     * @param cidAll
     *            当前订阅组的所有Consumer集合，无重复数据，且有序
     * @return 分配结果，无重复数据
     */
    public List<MessageQueue> allocate(//
            final String group,//
            final String topic,//
            final String currentCID,//
            final List<MessageQueue> mqAll,//
            final List<String> cidAll//
    );
}
