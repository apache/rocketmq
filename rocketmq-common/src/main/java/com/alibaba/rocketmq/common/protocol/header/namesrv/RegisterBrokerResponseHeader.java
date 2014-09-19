package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNullable;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-4
 */
public class RegisterBrokerResponseHeader implements CommandCustomHeader {
    @CFNullable
    private String haServerAddr;
    @CFNullable
    private String masterAddr;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getHaServerAddr() {
        return haServerAddr;
    }


    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }


    public String getMasterAddr() {
        return masterAddr;
    }


    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }
}
