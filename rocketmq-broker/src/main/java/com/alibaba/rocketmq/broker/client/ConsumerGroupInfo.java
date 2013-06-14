/**
 * $Id: ConsumerGroupInfo.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;


/**
 * 整个Consumer Group信息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ConsumerGroupInfo {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final String groupName;
    private volatile ConsumeType consumeType;
    private volatile MessageModel messageModel;
    private final ConcurrentHashMap<String/* Topic */, SubscriptionData> subscriptionTable =
            new ConcurrentHashMap<String, SubscriptionData>();
    private final ConcurrentHashMap<Integer/* channel id */, ClientChannelInfo> channelInfoTable =
            new ConcurrentHashMap<Integer/* channel id */, ClientChannelInfo>(16);

    private volatile long lastUpdateTimestamp = System.currentTimeMillis();


    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
    }


    public List<String> getAllClientId() {
        List<String> result = new ArrayList<String>();

        for (Integer id : this.channelInfoTable.keySet()) {
            ClientChannelInfo info = this.channelInfoTable.get(id);
            if (info != null) {
                result.add(info.getClientId());
            }
        }

        return result;
    }


    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel().id());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }


    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel.id());
        if (info != null) {
            log.warn(
                "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                info.toString(), groupName);
        }
    }


    /**
     * 返回值表示是否发生变更
     */
    public boolean updateChannel(final ClientChannelInfo clientChannelInfo, ConsumeType consumeType,
            MessageModel messageModel) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        ClientChannelInfo info = this.channelInfoTable.get(clientChannelInfo.getChannel().id());
        if (null == info) {
            ClientChannelInfo prev =
                    this.channelInfoTable.put(clientChannelInfo.getChannel().id(), clientChannelInfo);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                    messageModel, clientChannelInfo.toString());
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        return updated;
    }


    /**
     * 返回值表示是否发生变更
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
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
