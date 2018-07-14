package org.apache.rocketmq.common.message;

public enum DelayTimeLevel {
    // org.apache.rocketmq.store.config.MessageStoreConfig
    // messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    ONE_SECOND(1),
    FIVE_SECONDS(2),
    TEN_SECONDS(3),
    THIRTY_SECONDS(4),
    ONE_MINUTE(5),
    TWO_MINUTES(6),
    THREE_MINUTES(7),
    FOUR_MINUTES(8),
    FIVE_MINUTES(9),
    SIX_MINUTES(10),
    SEVEN_MINUTES(11),
    EIGHT_MINUTES(12),
    NINE_MINUTES(13),
    TEN_MINUTES(14),
    TWENTY_MINUTES(15),
    THIRTY_MINUTES(16),
    ONE_HOUR(17),
    TWO_HOURS(18);

    private int i;
    private DelayTimeLevel(int i){
        this.i = i;
    }

    public int getI() {
        return i;
    }

    public void setI(int i) {
        this.i = i;
    }
}
