package org.apache.rocketmq.common.protocol.header.namesrv.controller;

public class RegisterBrokerResponseHeader {
    private short errorCode;
    private String masterAddress;
    private int masterEpoch;
    // The id of this registered replicas.
    private long brokerId;

    public RegisterBrokerResponseHeader() {
    }

    public RegisterBrokerResponseHeader(String masterAddress, int masterEpoch, int brokerId) {
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.brokerId = brokerId;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public void setMasterEpoch(int masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }
}
