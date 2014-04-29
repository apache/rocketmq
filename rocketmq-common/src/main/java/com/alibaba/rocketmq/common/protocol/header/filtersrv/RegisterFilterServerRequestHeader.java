package com.alibaba.rocketmq.common.protocol.header.filtersrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class RegisterFilterServerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String filterServerAddr;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getFilterServerAddr() {
        return filterServerAddr;
    }


    public void setFilterServerAddr(String filterServerAddr) {
        this.filterServerAddr = filterServerAddr;
    }
}
