package org.apache.rocketmq.common.protocol.header.namesrv.controller;

public class RegisterBrokerRequestHeader {
    private String clusterName;
    private String brokerName;
    private String brokerAddress;

    public RegisterBrokerRequestHeader(String clusterName, String brokerName, String brokerAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }
}
