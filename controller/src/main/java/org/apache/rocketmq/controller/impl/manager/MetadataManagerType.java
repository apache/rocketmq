package org.apache.rocketmq.controller.impl.manager;

public enum MetadataManagerType {

    REPLICAS_INFO_MANAGER("ReplicasInfoManager", (short) 1);

    private final String name;
    private final short id;

    MetadataManagerType(String name, short id) {
        this.name = name;
        this.id = id;
    }

    public static MetadataManagerType from(short id) {
        switch (id) {
            case 1:
                return REPLICAS_INFO_MANAGER;
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public short getId() {
        return id;
    }
}
