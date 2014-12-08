package com.alibaba.rocketmq.common.protocol.header;

import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


public class ViewBrokerStatsDataRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String statsName;
    @CFNotNull
    private String statsKey;


    @Override
    public void checkFields() throws RemotingCommandException {

    }


    public String getStatsName() {
        return statsName;
    }


    public void setStatsName(String statsName) {
        this.statsName = statsName;
    }


    public String getStatsKey() {
        return statsKey;
    }


    public void setStatsKey(String statsKey) {
        this.statsKey = statsKey;
    }
}
