package org.apache.rocketmq.namesrv.controller.event;

import java.util.Set;

/**
 * The event alters the isr list of target broker.
 *
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/15 15:03
 */
public class AlterIsrEvent implements EventMessage {

    private String  brokerName;
    private Set<String/*Address*/> newIsr;

    public AlterIsrEvent(String brokerName, Set<String> newIsr) {
        this.brokerName = brokerName;
        this.newIsr = newIsr;
    }

    @Override
    public EventType eventType() {
        return EventType.ALTER_ISR_EVENT;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Set<String> getNewIsr() {
        return newIsr;
    }

    public void setNewIsr(Set<String> newIsr) {
        this.newIsr = newIsr;
    }
}
