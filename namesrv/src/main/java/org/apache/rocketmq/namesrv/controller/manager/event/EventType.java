package org.apache.rocketmq.namesrv.controller.manager.event;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/16 10:13
 */
public enum EventType {
    ALTER_SYNC_STATE_SET_EVENT("AlterSyncStateSetEvent", (short) 1),
    APPLY_BROKER_ID_EVENT("ApplyBrokerIdEvent", (short) 2),
    ELECT_MASTER_EVENT("ElectMasterEvent", (short) 3),
    READ_EVENT("ReadEvent", (short) 4);

    private final String name;
    private final short id;

    EventType(String name, short id) {
        this.name = name;
        this.id = id;
    }
}
