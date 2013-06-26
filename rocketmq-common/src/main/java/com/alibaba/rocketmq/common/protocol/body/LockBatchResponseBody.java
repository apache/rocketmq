package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-6-26
 */
public class LockBatchResponseBody extends RemotingSerializable {
    private Set<MessageQueue> mqSet = new HashSet<MessageQueue>();


    public Set<MessageQueue> getMqSet() {
        return mqSet;
    }


    public void setMqSet(Set<MessageQueue> mqSet) {
        this.mqSet = mqSet;
    }
}
