/**
 * $Id: MessageQueue.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.message;

/**
 * 消息队列数据结构，对外提供
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MessageQueue {
    private String topic;
    private String brokerName;
    private int queueId;


    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public int getQueueId() {
        return queueId;
    }


    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }


    @Override
    public String toString() {
        return "MessageQueue [topic=" + topic + ", brokerName=" + brokerName + ", queueId=" + queueId + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + queueId;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
        MessageQueue other = (MessageQueue) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        }
        else if (!brokerName.equals(other.brokerName))
            return false;
        if (queueId != other.queueId)
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        }
        else if (!topic.equals(other.topic))
            return false;
        return true;
    }
}
