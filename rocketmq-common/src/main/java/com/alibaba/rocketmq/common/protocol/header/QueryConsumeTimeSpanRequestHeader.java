package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 根据 topic 和 group 获取消息的时间跨度
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-12-30
 */
public class QueryConsumeTimeSpanRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String group;


    @Override
    public void checkFields() throws RemotingCommandException {
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
}
