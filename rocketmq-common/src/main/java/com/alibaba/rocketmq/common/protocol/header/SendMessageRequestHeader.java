/**
 * $Id: SendMessageRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SendMessageRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String producerGroup;
    @CFNotNull
    private String topic;
    @CFNotNull
    private String defaultTopic;
    @CFNotNull
    private Integer defaultTopicQueueNums;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Integer sysFlag;
    @CFNotNull
    private Long bornTimestamp;
    @CFNotNull
    private Integer flag;
    @CFNullable
    private String properties;
    @CFNullable
    private Integer reconsumeTimes;
    @CFNullable
    private boolean unitMode = false;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getDefaultTopic() {
        return defaultTopic;
    }


    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public Integer getSysFlag() {
        return sysFlag;
    }


    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }


    public Long getBornTimestamp() {
        return bornTimestamp;
    }


    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }


    public Integer getFlag() {
        return flag;
    }


    public void setFlag(Integer flag) {
        this.flag = flag;
    }


    public String getProperties() {
        return properties;
    }


    public void setProperties(String properties) {
        this.properties = properties;
    }


    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }


    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }
}
