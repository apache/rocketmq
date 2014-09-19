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
    // Lock成功的队列集合
    private Set<MessageQueue> lockOKMQSet = new HashSet<MessageQueue>();


    public Set<MessageQueue> getLockOKMQSet() {
        return lockOKMQSet;
    }


    public void setLockOKMQSet(Set<MessageQueue> lockOKMQSet) {
        this.lockOKMQSet = lockOKMQSet;
    }

}
