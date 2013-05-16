/**
 * $Id: TopicPublishInfo.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.common.MessageQueue;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class TopicPublishInfo {
    private boolean orderTopic = false;
    private List<MessageQueue> metaQueueList = new ArrayList<MessageQueue>();
    private AtomicInteger sendWhichQueue = new AtomicInteger(0);


    public boolean isOrderTopic() {
        return orderTopic;
    }


    public boolean ok() {
        return null != this.metaQueueList && !this.metaQueueList.isEmpty();
    }


    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }


    public List<MessageQueue> getMetaQueueList() {
        return metaQueueList;
    }


    public void setMetaQueueList(List<MessageQueue> metaQueueList) {
        this.metaQueueList = metaQueueList;
    }


    public AtomicInteger getSendWhichQueue() {
        return sendWhichQueue;
    }


    public void setSendWhichQueue(AtomicInteger sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }


    /**
     * 如果lastBrokerName不为null，则寻找与其不同的MetaQueue
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName != null) {
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.metaQueueList.size(); i++) {
                int pos = Math.abs(index++) % this.metaQueueList.size();
                MessageQueue mq = this.metaQueueList.get(pos);
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }

            return null;
        }
        else {
            int index = this.sendWhichQueue.getAndIncrement();
            int pos = Math.abs(index) % this.metaQueueList.size();
            return this.metaQueueList.get(pos);
        }
    }
}
