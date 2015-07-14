package com.alibaba.rocketmq.common.protocol.body;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageQueueForC;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @auther lansheng.zj
 */
public class ResetOffsetBodyForC extends RemotingSerializable {

    private List<MessageQueueForC> offsetTable;


    public List<MessageQueueForC> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(List<MessageQueueForC> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
