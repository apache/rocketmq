package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class ConsumeMessageDirectlyResultRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNullable
    private String clientId;
    @CFNullable
    private String msgId;
    @CFNullable
    private String brokerName;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public String getClientId() {
        return clientId;
    }


    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    public String getMsgId() {
        return msgId;
    }


    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
}
