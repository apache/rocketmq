package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class GetConsumeStatsInBrokerHeader implements CommandCustomHeader {
    @CFNotNull
    private boolean isOrder;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public boolean isOrder() {
        return isOrder;
    }

    public void setIsOrder(boolean isOrder) {
        this.isOrder = isOrder;
    }
}
