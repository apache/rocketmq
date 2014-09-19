package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-18
 */
public class ConsumerSendMsgBackRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private Long offset;
    @CFNotNull
    private String group;
    @CFNotNull
    private Integer delayLevel;
    private String originMsgId;
    private String originTopic;
    @CFNullable
    private boolean unitMode = false;


    @Override
    public void checkFields() throws RemotingCommandException {

    }


    public Long getOffset() {
        return offset;
    }


    public void setOffset(Long offset) {
        this.offset = offset;
    }


    public String getGroup() {
        return group;
    }


    public void setGroup(String group) {
        this.group = group;
    }


    public Integer getDelayLevel() {
        return delayLevel;
    }


    public void setDelayLevel(Integer delayLevel) {
        this.delayLevel = delayLevel;
    }


    public String getOriginMsgId() {
        return originMsgId;
    }


    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }


    public String getOriginTopic() {
        return originTopic;
    }


    public void setOriginTopic(String originTopic) {
        this.originTopic = originTopic;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }


    @Override
    public String toString() {
        return "ConsumerSendMsgBackRequestHeader [group=" + group + ", originTopic=" + originTopic
                + ", originMsgId=" + originMsgId + ", delayLevel=" + delayLevel + ", unitMode=" + unitMode
                + "]";
    }
}
