package org.apache.rocketmq.namesrv.controller.statemachine;

import java.util.HashSet;
import java.util.Set;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:33
 */
public class InSyncReplicasInfo {

    private final String clusterName;
    private final String brokerName;
    private Set<String/*Address*/> inSyncReplicas;
    private int isrEpoch;
    private String masterAddress;
    private int masterEpoch;
    // Because when a Broker becomes a master, its id needs to be assigned a value of 0.
    // We need to record it's originId so that when it becomes a follower again, we can find its original id.
    private long masterOriginId;

    public InSyncReplicasInfo(String clusterName, String brokerName, String masterAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterAddress = masterAddress;
        this.masterEpoch = 0;
        // The first master is the first online broker
        this.masterOriginId = 1;
        this.inSyncReplicas = new HashSet<>();
        this.inSyncReplicas.add(masterAddress);
        this.isrEpoch = 0;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Set<String> getInSyncReplicas() {
        return inSyncReplicas;
    }

    public void setInSyncReplicas(Set<String> inSyncReplicas) {
        this.inSyncReplicas = inSyncReplicas;
    }

    public int getIsrEpoch() {
        return isrEpoch;
    }

    public void setIsrEpoch(int isrEpoch) {
        this.isrEpoch = isrEpoch;
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

    public long getMasterOriginId() {
        return masterOriginId;
    }

    public void setMasterOriginId(long masterOriginId) {
        this.masterOriginId = masterOriginId;
    }
}
