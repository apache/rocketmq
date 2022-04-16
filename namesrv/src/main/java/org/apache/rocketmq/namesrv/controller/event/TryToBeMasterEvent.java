package org.apache.rocketmq.namesrv.controller.event;

/**
 * The event trys to be a master for target broker.
 * Triggered by GetReplicaInfo api, When the Broker queries the replica group information,
 * this event is triggered after the corresponding ISR information cannot be found.
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:06
 */
public class TryToBeMasterEvent implements EventMessage {
    private String clusterName;
    private String brokerName;
    private String brokerAddress;

    public TryToBeMasterEvent(String clusterName, String brokerName, String brokerAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
    }

    @Override
    public EventType eventType() {
        return EventType.TRY_TO_BE_MASTER_EVENT;
    }

    public String getClusterName() {
        return clusterName;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public String getBrokerAddress() {
        return brokerAddress;
    }
}
