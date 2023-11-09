package org.apache.rocketmq.common.resource;

public enum ResourceType {

    UNKNOWN("Unknown"),

    ANY("Any"),

    CLUSTER("Cluster"),

    NAMESPACE("Namespace"),

    TOPIC("Topic"),

    GROUP("Group"),

    /**
     * Only support resource with prefix, such as Topic:test
     */
    Resource("Resource"),

    /**
     * Only support subject with prefix, such as User:test
     */
    Subject("Subject"),

    USER("User");

    private final String code;

    ResourceType(String code) {
        this.code = code;
    }

    public static ResourceType getByCode(String type) {
        for (ResourceType resourceType : ResourceType.values()) {
            if (resourceType.getCode().equals(type)) {
                return resourceType;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }
}
