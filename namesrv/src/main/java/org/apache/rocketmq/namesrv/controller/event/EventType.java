package org.apache.rocketmq.namesrv.controller.event;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/16 10:13
 */
public enum EventType {
    ALTER_SYNC_STATE_SET_EVENT("AlterSyncStateSetEvent", (short) 1),
    APPLY_BROKER_ID_EVENT("ApplyBrokerIdEvent", (short) 2),
    ELECT_MASTER_EVENT("ElectMasterEvent", (short) 3),
    TRY_TO_BE_MASTER_EVENT("TryToBeMasterEvent", (short) 4),
    READ_EVENT("ReadEvent", (short) 5);

    private final String name;
    private final short id;

    EventType(String name, short id) {
        this.name = name;
        this.id = id;
    }
}
