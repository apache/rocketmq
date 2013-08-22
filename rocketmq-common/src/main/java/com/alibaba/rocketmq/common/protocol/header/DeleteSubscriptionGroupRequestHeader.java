package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * 删除订阅组请求参数
 * 
 * @author manhong.yqd<manhong.yqd@taobao.com>
 * @since 2013-8-22
 */
public class DeleteSubscriptionGroupRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String groupName;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
