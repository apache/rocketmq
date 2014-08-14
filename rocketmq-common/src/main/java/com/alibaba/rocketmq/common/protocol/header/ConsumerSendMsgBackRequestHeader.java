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
    private String msgId;
    private String topic;


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


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    @Override
    public String toString() {
        return "ConsumerSendMsgBackRequestHeader [group=" + group + ", topic=" + topic + ", msgId=" + msgId
                + ", delayLevel=" + delayLevel + "]";
    }
}
