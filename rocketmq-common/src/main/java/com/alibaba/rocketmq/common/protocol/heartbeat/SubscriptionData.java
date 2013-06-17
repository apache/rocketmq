/**
 * $Id: SubscriptionData.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SubscriptionData {
    public final static String SUB_ALL = "*";
    private String topic;
    private String subString;
    private final Set<String> tagsSet = new HashSet<String>();


    public SubscriptionData() {

    }


    public SubscriptionData(String topic, String subString) {
        super();
        this.topic = topic;
        this.subString = subString;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getSubString() {
        return subString;
    }


    public void setSubString(String subString) {
        this.subString = subString;
    }


    public Set<String> getTagsSet() {
        return tagsSet;
    }


    @Override
    public String toString() {
        return "SubscriptionData [topic=" + topic + ", subString=" + subString + ", tagsSet=" + tagsSet + "]";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + ((tagsSet == null) ? 0 : tagsSet.hashCode());
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
        SubscriptionData other = (SubscriptionData) obj;
        if (subString == null) {
            if (other.subString != null)
                return false;
        }
        else if (!subString.equals(other.subString))
            return false;
        if (tagsSet == null) {
            if (other.tagsSet != null)
                return false;
        }
        else if (!tagsSet.equals(other.tagsSet))
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
