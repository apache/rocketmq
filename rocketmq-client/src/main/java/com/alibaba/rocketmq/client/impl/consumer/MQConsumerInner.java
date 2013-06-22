/**
 * $Id: MQConsumerInner.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.Set;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface MQConsumerInner {
    public String groupName();


    public MessageModel messageModel();


    public ConsumeType consumeType();


    public ConsumeFromWhere consumeFromWhere();


    public Set<SubscriptionData> subscriptions();


    public void doRebalance();


    public void persistConsumerOffset();


    public void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);
}
