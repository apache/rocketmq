/**
 * $Id: TopicPublishInfo.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.common.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class TopicPublishInfo {
    private boolean orderTopic = false;
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    private AtomicInteger sendWhichQueue = new AtomicInteger(0);


    public boolean isOrderTopic() {
        return orderTopic;
    }


    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }


    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }


    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }


    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }


    public AtomicInteger getSendWhichQueue() {
        return sendWhichQueue;
    }


    public void setSendWhichQueue(AtomicInteger sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }


    /**
     * 如果lastBrokerName不为null，则寻找与其不同的MessageQueue
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName != null) {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.messageQueueList.size();
                MessageQueue mq = this.messageQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }

            return null;
        }
        else {
            int index = this.sendWhichQueue.getAndIncrement();
            int pos = Math.abs(index) % this.messageQueueList.size();
            return this.messageQueueList.get(pos);
        }
    }
}
