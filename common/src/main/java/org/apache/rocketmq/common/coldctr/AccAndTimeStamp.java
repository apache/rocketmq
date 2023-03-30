package org.apache.rocketmq.common.coldctr;

import java.util.concurrent.atomic.AtomicLong;

public class AccAndTimeStamp {

    public AtomicLong coldAcc = new AtomicLong(0L);
    public Long lastColdReadTimeMills = System.currentTimeMillis();
    public Long createTimeMills = System.currentTimeMillis();

    public AccAndTimeStamp(AtomicLong coldAcc) {
        this.coldAcc = coldAcc;
    }

    public AtomicLong getColdAcc() {
        return coldAcc;
    }

    public void setColdAcc(AtomicLong coldAcc) {
        this.coldAcc = coldAcc;
    }

    public Long getLastColdReadTimeMills() {
        return lastColdReadTimeMills;
    }

    public void setLastColdReadTimeMills(Long lastColdReadTimeMills) {
        this.lastColdReadTimeMills = lastColdReadTimeMills;
    }

    public Long getCreateTimeMills() {
        return createTimeMills;
    }

    public void setCreateTimeMills(Long createTimeMills) {
        this.createTimeMills = createTimeMills;
    }

    @Override
    public String toString() {
        return "AccAndTimeStamp{" +
            "coldAcc=" + coldAcc +
            ", lastColdReadTimeMills=" + lastColdReadTimeMills +
            ", createTimeMills=" + createTimeMills +
            '}';
    }
}
