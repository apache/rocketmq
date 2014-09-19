package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 查看客户端消费组的消费情况。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-12-30
 */
public class GetConsumerStatusRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String group;
    @CFNullable
    private String clientAddr;


    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getGroup() {
        return group;
    }


    public void setGroup(String group) {
        this.group = group;
    }


    public String getClientAddr() {
        return clientAddr;
    }


    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }
}
