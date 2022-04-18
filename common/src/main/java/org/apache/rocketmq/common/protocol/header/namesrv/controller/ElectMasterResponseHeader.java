package org.apache.rocketmq.common.protocol.header.namesrv.controller;


public class ElectMasterResponseHeader {
    private short errorCode = ErrorCodes.NONE.getCode();
    private String newMasterAddress;
    private int masterEpoch;

    public ElectMasterResponseHeader() {
    }

    public ElectMasterResponseHeader(String newMasterAddress, int masterEpoch) {
        this.newMasterAddress = newMasterAddress;
        this.masterEpoch = masterEpoch;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public String getNewMasterAddress() {
        return newMasterAddress;
    }

    public void setNewMasterAddress(String newMasterAddress) {
        this.newMasterAddress = newMasterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public void setMasterEpoch(int masterEpoch) {
        this.masterEpoch = masterEpoch;
    }
}
