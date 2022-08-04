package org.apache.rocketmq.store.ha.netty;

public enum TransferType {

    UNKNOWN(0),

    /**
     * Request to add a broker as slave
     * format: protocol version + language + brokerName + brokerId + brokerPerm
     */
    HANDSHAKE_SLAVE(1),

    /**
     * Master return the result of handshake
     * format: protocol version + language + brokerName + brokerId + brokerPerm
     */
    HANDSHAKE_MASTER(2),

    /**
     * query master epoch
     */
    QUERY_EPOCH(3),

    /**
     * master broker reply epoch
     */
    RETURN_EPOCH(4),

    /**
     * slave broker truncate self log and send signal to master, then master push data
     */
    CONFIRM_TRUNCATE(5),

    /**
     * Master broker transfer commitlog data to slave broker
     * format: current epoch, confirm offset, pull from offset, block size, content(large)
     */
    TRANSFER_DATA(6),

    /**
     * Slave broker report receive offset to master
     */
    TRANSFER_ACK(7);

    private final int value;

    TransferType(int value) {
        this.value = value;
    }

    public static TransferType valueOf(int code) {
        for (TransferType tmp : TransferType.values()) {
            if (tmp.getValue() == code) {
                return tmp;
            }
        }
        return UNKNOWN;
    }

    public int getValue() {
        return value;
    }
}
