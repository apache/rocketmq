package org.apache.rocketmq.namesrv.controller.manager.event;

import java.util.Set;

/**
 * The event alters the isr list of target broker.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:03
 */
public class AlterSyncStateSetEvent implements EventMessage {

    private final String brokerName;
    private final Set<String/*Address*/> newSyncStateSet;

    public AlterSyncStateSetEvent(String brokerName, Set<String> newSyncStateSet) {
        this.brokerName = brokerName;
        this.newSyncStateSet = newSyncStateSet;
    }

    @Override
    public EventType eventType() {
        return EventType.ALTER_SYNC_STATE_SET_EVENT;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Set<String> getNewSyncStateSet() {
        return newSyncStateSet;
    }
}
