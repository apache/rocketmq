package org.apache.rocketmq.common.protocol.header.namesrv.controller;

import java.util.Set;

public class AlterInSyncReplicasResponseHeader {
    private short errorCode = ErrorCodes.NONE.getCode();
    private Set<String> newSyncStateSet;
    private int newSyncStateSetEpoch;

    public AlterInSyncReplicasResponseHeader() {
    }

    public AlterInSyncReplicasResponseHeader(Set<String> newSyncStateSet, int newSyncStateSetEpoch) {
        this.newSyncStateSet = newSyncStateSet;
        this.newSyncStateSetEpoch = newSyncStateSetEpoch;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public Set<String> getNewSyncStateSet() {
        return newSyncStateSet;
    }

    public void setNewSyncStateSet(Set<String> newSyncStateSet) {
        this.newSyncStateSet = newSyncStateSet;
    }

    public int getNewSyncStateSetEpoch() {
        return newSyncStateSetEpoch;
    }

    public void setNewSyncStateSetEpoch(int newSyncStateSetEpoch) {
        this.newSyncStateSetEpoch = newSyncStateSetEpoch;
    }
}
