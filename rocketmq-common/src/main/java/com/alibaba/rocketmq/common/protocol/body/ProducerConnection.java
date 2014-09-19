package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashSet;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * TODO
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 13-8-5
 */
public class ProducerConnection extends RemotingSerializable {
    private HashSet<Connection> connectionSet = new HashSet<Connection>();


    public HashSet<Connection> getConnectionSet() {
        return connectionSet;
    }


    public void setConnectionSet(HashSet<Connection> connectionSet) {
        this.connectionSet = connectionSet;
    }
}
