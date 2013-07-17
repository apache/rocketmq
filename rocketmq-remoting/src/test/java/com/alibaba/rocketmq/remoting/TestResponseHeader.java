package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class TestResponseHeader implements CommandCustomHeader {
    @CFNullable
    private Integer count;

    @CFNullable
    private String messageTitle;


    @Override
    public void checkFields() throws RemotingCommandException {

    }


    public Integer getCount() {
        return count;
    }


    public void setCount(Integer count) {
        this.count = count;
    }


    public String getMessageTitle() {
        return messageTitle;
    }


    public void setMessageTitle(String messageTitle) {
        this.messageTitle = messageTitle;
    }
}
