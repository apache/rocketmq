/**
 * $Id: ConsumerGroupInfo.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MetaMix;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * 整个Consumer Group信息
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ConsumerGroupInfo {
    private static final Logger log = LoggerFactory.getLogger(MetaMix.BrokerLoggerName);

    private final String groupName;
    private volatile ConsumeType consumeType;
    private volatile MessageModel messageModel;
    private final ConcurrentHashMap<String/* Topic */, SubscriptionData> subscriptionTable =
            new ConcurrentHashMap<String, SubscriptionData>();
    private final ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable =
            new ConcurrentHashMap<Channel, ClientChannelInfo>(16);

    private volatile long lastUpdateTimestamp = System.currentTimeMillis();


    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
    }


    /**
     * 返回值表示是否发生变更
     */
    public boolean updateChannel(final ClientChannelInfo clientChannelInfo, ConsumeType consumeType,
            MessageModel messageModel) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        ClientChannelInfo info = this.channelInfoTable.get(clientChannelInfo.getChannel());
        if (info != null) {
            ClientChannelInfo prev = this.channelInfoTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            if (null == prev) {
                log.info("receive new channel, " + clientChannelInfo + ", consumer group: " + this.groupName);
                updated = true;
            }
        }

        for (Channel c : this.channelInfoTable.keySet()) {
            if (!c.isActive()) {
                log.info("the channel is not active, remove it, " + c.remoteAddress() + ", consumer group: "
                        + this.groupName);
                this.channelInfoTable.remove(c);
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        return updated;
    }


    /**
     * 返回值表示是否发生变更
     */
    public boolean updateSubscription(final List<SubscriptionData> subList) {
        boolean updated = false;
        for (SubscriptionData sub : subList) {
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            if (old == null) {
                SubscriptionData prev = this.subscriptionTable.put(sub.getTopic(), sub);
                if (null == prev) {
                    updated = true;
                }
            }
            else if (!sub.equals(old)) {
                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        // TODO 是否需要删除多余的订阅关系，不删除似乎也没啥影响
        // 这里如果确实有topic取消订阅了， 应该返回true

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }


    public ConsumeType getConsumeType() {
        return consumeType;
    }


    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }


    public MessageModel getMessageModel() {
        return messageModel;
    }


    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }


    public String getGroupName() {
        return groupName;
    }


    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }


    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }
}
