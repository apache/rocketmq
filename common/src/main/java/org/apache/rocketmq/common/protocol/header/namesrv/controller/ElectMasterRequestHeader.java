package org.apache.rocketmq.common.protocol.header.namesrv.controller;

public class ElectMasterRequestHeader {
    private String brokerName;

    public ElectMasterRequestHeader(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
