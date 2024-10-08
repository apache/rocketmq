package org.apache.rocketmq.common.coldctr;

import java.util.concurrent.atomic.AtomicLong;

public class AccAndTimeStamp {

    private final AtomicLong coldAcc; // Made this final since it should be initialized once
    private long lastColdReadTimeMillis; // Renamed for consistency
    private final long createTimeMillis; // Made this final to indicate it is immutable after creation

    // Constructor initializes coldAcc and sets timestamps
    public AccAndTimeStamp(AtomicLong coldAcc) {
        this.coldAcc = coldAcc;
        this.lastColdReadTimeMillis = System.currentTimeMillis();
        this.createTimeMillis = System.currentTimeMillis();
    }

    // Getters
    public AtomicLong getColdAcc() {
        return coldAcc;
    }

    public long getLastColdReadTimeMillis() {
        return lastColdReadTimeMillis;
    }

    public void setLastColdReadTimeMillis(long lastColdReadTimeMillis) {
        this.lastColdReadTimeMillis = lastColdReadTimeMillis;
    }

    public long getCreateTimeMillis() {
        return createTimeMillis;
    }

    @Override
    public String toString() {
        return "AccAndTimeStamp{" +
            "coldAcc=" + coldAcc +
            ", lastColdReadTimeMillis=" + lastColdReadTimeMillis +
            ", createTimeMillis=" + createTimeMillis +
            '}';
    }
}
