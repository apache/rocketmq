/**
 * $Id: PullMessageRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PullMessageRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNotNull
    private String topic;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Long queueOffset;
    @CFNotNull
    private Integer maxMsgNums;
    @CFNotNull
    private Integer sysFlag;
    @CFNotNull
    private Long commitOffset;
    @CFNotNull
    private Long suspendTimeoutMillis;
    @CFNullable
    private String subscription;
    @CFNotNull
    private Long subVersion;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public Integer getQueueId() {
        return queueId;
    }


    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }


    public Long getQueueOffset() {
        return queueOffset;
    }


    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }


    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }


    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }


    public Integer getSysFlag() {
        return sysFlag;
    }


    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }


    public Long getCommitOffset() {
        return commitOffset;
    }


    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }


    public Long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }


    public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
    }


    public String getSubscription() {
        return subscription;
    }


    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }


    public Long getSubVersion() {
        return subVersion;
    }


    public void setSubVersion(Long subVersion) {
        this.subVersion = subVersion;
    }
}
