package org.apache.rocketmq.store.ha.netty;

/**
 * 还有增加 preferred leader 默认为 ISR 中 min offset 最小的 broker
 * 固定 isr brokerId
 */
public enum HAMessageType {

    UNKNOWN(0),

    /**
     * 注册一个新的备, protocol version + language + brokerName + brokerId + brokerPerm
     */
    SLAVE_HANDSHAKE(1),

    /**
     * 服务端接受这个备的注册, 并返回 protocol version + language + brokerName + brokerId + brokerPerm
     */
    MASTER_HANDSHAKE(2),

    /**
     * 备会阻塞，使用这个请求同步查询 epoch 信息
     */
    QUERY_EPOCH(3),

    /**
     * 主一次性返回备所有 epoch 信息
     */
    RETURN_EPOCH(4),

    /**
     * 备截断 commitLog 并向主报告这个状态
     */
    CONFIRM_TRUNCATE(5),

    /**
     * 主向备推送数据, current epoch, confirm offset, pull from offset, block size, content(large)
     */
    PUSH_DATA(6),

    /**
     * 备确认
     */
    PUSH_ACK(7);

    private final int value;

    HAMessageType(int value) {
        this.value = value;
    }

    public static HAMessageType valueOf(int code) {
        for (HAMessageType tmp : HAMessageType.values()) {
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
