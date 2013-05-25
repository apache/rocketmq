/**
 * $Id: PullRequest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.common.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullRequest {
    private String consumerGroup;
    private MessageQueue messageQueue;
    private long nextOffset;


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


    public long getNextOffset() {
        return nextOffset;
    }


    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }
}
