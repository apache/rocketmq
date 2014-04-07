package com.alibaba.rocketmq.common.protocol.header.filtersrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class RegisterMessageFilterClassRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNotNull
    private String className;
    @CFNotNull
    private Integer classCRC;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    public String getClassName() {
        return className;
    }


    public void setClassName(String className) {
        this.className = className;
    }


    public Integer getClassCRC() {
        return classCRC;
    }


    public void setClassCRC(Integer classCRC) {
        this.classCRC = classCRC;
    }
}
