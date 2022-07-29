package org.apache.rocketmq.store.ha.protocol;

public enum HandshakeResult {

    /**
     * master accept slave establish connection
     */
    ACCEPT(0),

    /**
     * master reject slave establish connection
     */
    REJECT(-1),

    /**
     * identity validate failed due to cluster name incorrect
     */
    CLUSTER_NAME_NOT_MATCH(-2),

    /**
     * identity validate failed due to broker name incorrect
     */
    BROKER_NAME_NOT_MATCH(-3),

    /**
     * broker id incorrect
     */
    BROKER_ID_ERROR(-4),

    /**
     * master not support this ha protocol
     */
    PROTOCOL_NOT_SUPPORT(-5),

    /**
     * master app version not support
     */
    APP_VERSION_NOT_SUPPORT(-6);

    private final int value;

    HandshakeResult(int value) {
        this.value = value;
    }

    public static HandshakeResult valueOf(int code) {
        for (HandshakeResult tmp : HandshakeResult.values()) {
            if (tmp.getValue() == code) {
                return tmp;
            }
        }
        return REJECT;
    }

    public int getValue() {
        return value;
    }
}
