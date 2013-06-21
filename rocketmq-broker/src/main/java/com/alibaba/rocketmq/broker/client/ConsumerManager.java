/**
 * $Id: ConsumerManager.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * Consumer连接、订阅关系管理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ConsumerManager {
    private final ConcurrentHashMap<String/* Group */, ConsumerGroupInfo> consumerTable =
            new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);


    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }


    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        for (String group : this.consumerTable.keySet()) {
            final ConsumerGroupInfo info = this.consumerTable.get(group);
            if (info != null) {
                info.doChannelCloseEvent(remoteAddr, channel);
            }
        }
    }


    /**
     * 返回是否有变化
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
            ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
            final Set<SubscriptionData> subList) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        boolean r1 =
                consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                    consumeFromWhere);
        boolean r2 = consumerGroupInfo.updateSubscription(subList);
        return r1 || r2;
    }


    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
        }
    }
}
