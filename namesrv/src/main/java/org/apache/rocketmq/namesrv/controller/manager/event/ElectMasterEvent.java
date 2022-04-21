package org.apache.rocketmq.namesrv.controller.manager.event;

/**
 * The event trys to elect a new master for target broker.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:04
 */
public class ElectMasterEvent implements EventMessage {
    // Mark whether a new master was elected.
    private boolean newMasterElected;
    private String brokerName;
    private String newMasterAddress;
    private String clusterName;

    public ElectMasterEvent(boolean newMasterElected, String brokerName) {
        this(newMasterElected, brokerName, "", "");
    }

    public ElectMasterEvent(String brokerName, String newMasterAddress) {
        this(true, brokerName, newMasterAddress, "");
    }

    public ElectMasterEvent(boolean newMasterElected, String brokerName, String newMasterAddress,
        String clusterName) {
        this.newMasterElected = newMasterElected;
        this.brokerName = brokerName;
        this.newMasterAddress = newMasterAddress;
        this.clusterName = clusterName;
    }

    @Override
    public EventType getEventType() {
        return EventType.ELECT_MASTER_EVENT;
    }

    public boolean getNewMasterElected() {
        return newMasterElected;
    }

    public void setNewMasterElected(boolean newMasterElected) {
        this.newMasterElected = newMasterElected;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getNewMasterAddress() {
        return newMasterAddress;
    }

    public void setNewMasterAddress(String newMasterAddress) {
        this.newMasterAddress = newMasterAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override public String toString() {
        return "ElectMasterEvent{" +
            "isNewMasterElected=" + newMasterElected +
            ", brokerName='" + brokerName + '\'' +
            ", newMasterAddress='" + newMasterAddress + '\'' +
            ", clusterName='" + clusterName + '\'' +
            '}';
    }
}
