package org.apache.rocketmq.namesrv.controller.event;

/**
 * The event trys to elect a new master for target broker.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:04
 */
public class ElectMasterEvent implements EventMessage {
    private String brokerName;
    private String newMasterAddress;

    public ElectMasterEvent(String brokerName, String newMasterAddress) {
        this.brokerName = brokerName;
        this.newMasterAddress = newMasterAddress;
    }

    @Override
    public EventType eventType() {
        return EventType.ELECT_MASTER_EVENT;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getNewMasterAddress() {
        return newMasterAddress;
    }

}
