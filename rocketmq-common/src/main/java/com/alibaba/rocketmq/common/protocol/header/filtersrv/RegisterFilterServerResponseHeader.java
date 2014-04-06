package com.alibaba.rocketmq.common.protocol.header.filtersrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class RegisterFilterServerResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerRole;
    @CFNotNull
    private long brokerId;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getBrokerRole() {
        return brokerRole;
    }


    public void setBrokerRole(String brokerRole) {
        this.brokerRole = brokerRole;
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }
}
