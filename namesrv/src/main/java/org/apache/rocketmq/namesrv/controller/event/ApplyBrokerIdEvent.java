package org.apache.rocketmq.namesrv.controller.event;

/**
 * The event trys to apply a new id for a new broker.
 * Triggered by the GetReplicaInfo API, this event is Triggered when the broker
 * cannot find its own brokerId when querying the replica group information
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:04
 */
public class ApplyBrokerIdEvent implements EventMessage {
    private final String brokerName;
    private final String brokerAddress;
    private final long newBrokerId;

    public ApplyBrokerIdEvent(String brokerName, String brokerAddress, long newBrokerId) {
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
        this.newBrokerId = newBrokerId;
    }

    @Override
    public EventType eventType() {
        return EventType.APPLY_BROKER_ID_EVENT;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }


    public long getNewBrokerId() {
        return newBrokerId;
    }
}
