package org.apache.rocketmq.acl2.enums;

public enum ResourceType {

    CLUSTER("Cluster"),

    NAMESPACE("Namespace"),

    TOPIC("Topic"),

    GROUP("Group"),

    USER("User"),

    ACL("Acl");

    private final String code;

    ResourceType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
