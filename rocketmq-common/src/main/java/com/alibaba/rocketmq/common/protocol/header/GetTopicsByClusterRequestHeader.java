package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 14-03-05
 */
public class GetTopicsByClusterRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String cluster;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getCluster() {
        return cluster;
    }


    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
