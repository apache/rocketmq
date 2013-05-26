/**
 * $Id: MQConsumerInner.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.Set;

import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public interface MQConsumerInner {
    public String getGroupName();


    public MessageModel getMessageModel();


    public ConsumeType getConsumeType();


    public Set<SubscriptionData> getMQSubscriptions();


    public void uploadConsumerOffsetsToBroker();
}
