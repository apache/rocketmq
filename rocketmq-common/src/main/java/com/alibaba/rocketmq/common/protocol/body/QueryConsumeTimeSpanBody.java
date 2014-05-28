package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;


/**
 * 查看客户端消费组的消费情况。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-12-30
 */
public class QueryConsumeTimeSpanBody extends RemotingSerializable {
    Set<QueueTimeSpan> consumeTimeSpanSet = new HashSet<QueueTimeSpan>();


    public Set<QueueTimeSpan> getConsumeTimeSpanSet() {
        return consumeTimeSpanSet;
    }


    public void setConsumeTimeSpanSet(Set<QueueTimeSpan> consumeTimeSpanSet) {
        this.consumeTimeSpanSet = consumeTimeSpanSet;
    }
}
