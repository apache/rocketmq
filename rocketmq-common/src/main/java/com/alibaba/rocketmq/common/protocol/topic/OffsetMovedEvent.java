package com.alibaba.rocketmq.common.protocol.topic;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


public class OffsetMovedEvent extends RemotingSerializable {
    private String consumerGroup;
    private MessageQueue messageQueue;
    /**
     * 客户端请求的Offset
     */
    private long offsetRequest;
    /**
     * Broker要求从这个新的Offset开始消费
     */
    private long offsetNew;


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public MessageQueue getMessageQueue() {
        return messageQueue;
    }


    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }


    public long getOffsetRequest() {
        return offsetRequest;
    }


    public void setOffsetRequest(long offsetRequest) {
        this.offsetRequest = offsetRequest;
    }


    public long getOffsetNew() {
        return offsetNew;
    }


    public void setOffsetNew(long offsetNew) {
        this.offsetNew = offsetNew;
    }


    @Override
    public String toString() {
        return "OffsetMovedEvent [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
                + ", offsetRequest=" + offsetRequest + ", offsetNew=" + offsetNew + "]";
    }
}
