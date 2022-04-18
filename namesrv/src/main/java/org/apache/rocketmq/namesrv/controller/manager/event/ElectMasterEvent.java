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
    private boolean isNewMasterElected;
    private String brokerName;
    private String newMasterAddress;
    private String clusterName;

    public ElectMasterEvent(String brokerName, boolean isNewMasterElected) {
        this.isNewMasterElected = isNewMasterElected;
        this.brokerName = brokerName;
        this.newMasterAddress = "";
    }

    public ElectMasterEvent(String brokerName, String newMasterAddress) {
        this.isNewMasterElected = true;
        this.brokerName = brokerName;
        this.newMasterAddress = newMasterAddress;
    }

    @Override
    public EventType eventType() {
        return EventType.ELECT_MASTER_EVENT;
    }

    public boolean isNewMasterElected() {
        return isNewMasterElected;
    }

    public void setNewMasterElected(boolean newMasterElected) {
        isNewMasterElected = newMasterElected;
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
}
