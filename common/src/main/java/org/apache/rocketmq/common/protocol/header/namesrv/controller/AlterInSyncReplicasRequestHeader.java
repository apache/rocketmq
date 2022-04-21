package org.apache.rocketmq.common.protocol.header.namesrv.controller;

import java.util.Set;

public class AlterInSyncReplicasRequestHeader {
    private String brokerName;
    private String masterAddress;
    private int masterEpoch;
    private Set<String> newSyncStateSet;
    private int syncStateSetEpoch;

    public AlterInSyncReplicasRequestHeader(String brokerName, String masterAddress, int masterEpoch,
        Set<String> newSyncStateSet, int syncStateSetEpoch) {
        this.brokerName = brokerName;
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.newSyncStateSet = newSyncStateSet;
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
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

    public Set<String> getNewSyncStateSet() {
        return newSyncStateSet;
    }

    public void setNewSyncStateSet(Set<String> newSyncStateSet) {
        this.newSyncStateSet = newSyncStateSet;
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(int syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    @Override public String toString() {
        return "AlterInSyncReplicasRequestHeader{" +
            "brokerName='" + brokerName + '\'' +
            ", masterAddress='" + masterAddress + '\'' +
            ", masterEpoch=" + masterEpoch +
            ", newSyncStateSet=" + newSyncStateSet +
            ", syncStateSetEpoch=" + syncStateSetEpoch +
            '}';
    }
}
