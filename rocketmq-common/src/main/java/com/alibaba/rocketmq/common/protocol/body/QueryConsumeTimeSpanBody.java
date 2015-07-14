package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;


/**
 * 查看客户端消费组的消费情况。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-12-30
 */
public class QueryConsumeTimeSpanBody extends RemotingSerializable {
    List<QueueTimeSpan> consumeTimeSpanSet = new ArrayList<QueueTimeSpan>();


    public List<QueueTimeSpan> getConsumeTimeSpanSet() {
        return consumeTimeSpanSet;
    }


    public void setConsumeTimeSpanSet(List<QueueTimeSpan> consumeTimeSpanSet) {
        this.consumeTimeSpanSet = consumeTimeSpanSet;
    }
}
