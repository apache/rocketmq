package org.apache.rocketmq.namesrv.controller.event;

/**
 * The event trys to elect a new master for target broker.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:04
 */
public class ElectMasterEvent implements EventMessage {
    // Mark whether a new master was elected.
    private final boolean isNewMasterElected;
    private final String brokerName;
    private final String newMasterAddress;

    public ElectMasterEvent(boolean isNewMasterElected) {
        this.isNewMasterElected = isNewMasterElected;
        this.brokerName = "";
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

    public String getBrokerName() {
        return brokerName;
    }

    public String getNewMasterAddress() {
        return newMasterAddress;
    }

}
