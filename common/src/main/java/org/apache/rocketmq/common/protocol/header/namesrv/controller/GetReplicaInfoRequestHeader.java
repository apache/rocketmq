package org.apache.rocketmq.common.protocol.header.namesrv.controller;

public class GetReplicaInfoRequestHeader {
    private String brokerName;

    public GetReplicaInfoRequestHeader(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    @Override public String toString() {
        return "GetReplicaInfoRequestHeader{" +
            "brokerName='" + brokerName + '\'' +
            '}';
    }
}
