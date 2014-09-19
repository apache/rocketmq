/**
 * $Id: ConsumerData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ConsumerData {
    private String groupName;
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private ConsumeFromWhere consumeFromWhere;
    private Set<SubscriptionData> subscriptionDataSet = new HashSet<SubscriptionData>();
    private boolean unitMode;


    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
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


    public Set<SubscriptionData> getSubscriptionDataSet() {
        return subscriptionDataSet;
    }


    public void setSubscriptionDataSet(Set<SubscriptionData> subscriptionDataSet) {
        this.subscriptionDataSet = subscriptionDataSet;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }


    @Override
    public String toString() {
        return "ConsumerData [groupName=" + groupName + ", consumeType=" + consumeType + ", messageModel="
                + messageModel + ", consumeFromWhere=" + consumeFromWhere + ", unitMode=" + unitMode
                + ", subscriptionDataSet=" + subscriptionDataSet + "]";
    }
}
