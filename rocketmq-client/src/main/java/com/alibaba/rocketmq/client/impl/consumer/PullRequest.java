/**
 * $Id: PullRequest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PullRequest {
    private String consumerGroup;
    private MessageQueue messageQueue;
    // hashCode与equals方法不包含此字段
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


    @Override
    public String toString() {
        return "PullRequest [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue + ", nextOffset="
                + nextOffset + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerGroup == null) ? 0 : consumerGroup.hashCode());
        result = prime * result + ((messageQueue == null) ? 0 : messageQueue.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PullRequest other = (PullRequest) obj;
        if (consumerGroup == null) {
            if (other.consumerGroup != null)
                return false;
        }
        else if (!consumerGroup.equals(other.consumerGroup))
            return false;
        if (messageQueue == null) {
            if (other.messageQueue != null)
                return false;
        }
        else if (!messageQueue.equals(other.messageQueue))
            return false;
        return true;
    }
}
