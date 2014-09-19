package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * TODO
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 13-8-5
 */
public class ConsumerConnection extends RemotingSerializable {
    private HashSet<Connection> connectionSet = new HashSet<Connection>();
    private ConcurrentHashMap<String/* Topic */, SubscriptionData> subscriptionTable =
            new ConcurrentHashMap<String, SubscriptionData>();
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private ConsumeFromWhere consumeFromWhere;


    public int computeMinVersion() {
        int minVersion = Integer.MAX_VALUE;
        for (Connection c : this.connectionSet) {
            if (c.getVersion() < minVersion) {
                minVersion = c.getVersion();
            }
        }

        return minVersion;
    }


    public HashSet<Connection> getConnectionSet() {
        return connectionSet;
    }


    public void setConnectionSet(HashSet<Connection> connectionSet) {
        this.connectionSet = connectionSet;
    }


    public ConcurrentHashMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }


    public void setSubscriptionTable(ConcurrentHashMap<String, SubscriptionData> subscriptionTable) {
        this.subscriptionTable = subscriptionTable;
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


    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }


    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
