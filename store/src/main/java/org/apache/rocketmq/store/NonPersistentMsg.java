package org.apache.rocketmq.store;

public class NonPersistentMsg {
    String topic;
    int queueId;
    String messageProperties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getMessageProperties() {
        return messageProperties;
    }

    public void setMessageProperties(String messageProperties) {
        this.messageProperties = messageProperties;
    }
}
