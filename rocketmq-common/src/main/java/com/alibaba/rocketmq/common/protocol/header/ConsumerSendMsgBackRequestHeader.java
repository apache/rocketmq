package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
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
    // 重试之前的topic
    @CFNotNull
    private String prevTopic;


    @Override
    public void checkFields() throws RemotingCommandException {
        if (this.delayLevel <= 0) {
            throw new RemotingCommandException("delayLevel <= 0");
        }
    }


    public long getOffset() {
        return offset;
    }


    public void setOffset(long offset) {
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


    public String getPrevTopic() {
        return prevTopic;
    }


    public void setPrevTopic(String prevTopic) {
        this.prevTopic = prevTopic;
    }


    @Override
    public String toString() {
        return "ConsumerSendMsgBackRequestHeader [offset=" + offset + ", group=" + group + ", delayLevel="
                + delayLevel + ", prevTopic=" + prevTopic + "]";
    }
}
