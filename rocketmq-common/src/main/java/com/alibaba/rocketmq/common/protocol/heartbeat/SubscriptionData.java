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
    private boolean classFilterMode = false;
    private String topic;
    private String subString;
    private Set<String> tagsSet = new HashSet<String>();
    private Set<Integer> codeSet = new HashSet<Integer>();
    private long subVersion = System.currentTimeMillis();


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


    public void setTagsSet(Set<String> tagsSet) {
        this.tagsSet = tagsSet;
    }


    public long getSubVersion() {
        return subVersion;
    }


    public void setSubVersion(long subVersion) {
        this.subVersion = subVersion;
    }


    public Set<Integer> getCodeSet() {
        return codeSet;
    }


    public void setCodeSet(Set<Integer> codeSet) {
        this.codeSet = codeSet;
    }


    public boolean isClassFilterMode() {
        return classFilterMode;
    }


    public void setClassFilterMode(boolean classFilterMode) {
        this.classFilterMode = classFilterMode;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (classFilterMode ? 1231 : 1237);
        result = prime * result + ((codeSet == null) ? 0 : codeSet.hashCode());
        result = prime * result + ((subString == null) ? 0 : subString.hashCode());
        result = prime * result + (int) (subVersion ^ (subVersion >>> 32));
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
        if (classFilterMode != other.classFilterMode)
            return false;
        if (codeSet == null) {
            if (other.codeSet != null)
                return false;
        }
        else if (!codeSet.equals(other.codeSet))
            return false;
        if (subString == null) {
            if (other.subString != null)
                return false;
        }
        else if (!subString.equals(other.subString))
            return false;
        if (subVersion != other.subVersion)
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


    @Override
    public String toString() {
        return "SubscriptionData [classFilterMode=" + classFilterMode + ", topic=" + topic + ", subString="
                + subString + ", tagsSet=" + tagsSet + ", codeSet=" + codeSet + ", subVersion=" + subVersion
                + "]";
    }
}
