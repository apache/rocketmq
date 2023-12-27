package org.apache.rocketmq.common.resource;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

public enum ResourceType {

    UNKNOWN((byte) 0, "Unknown"),

    ANY((byte) 1, "Any"),

    CLUSTER((byte) 2, "Cluster"),

    NAMESPACE((byte) 3, "Namespace"),

    TOPIC((byte) 4, "Topic"),

    GROUP((byte) 5, "Group"),

    /**
     * Only support resource with prefix, such as Topic:test
     */
    Resource((byte) 6, "Resource"),

    /**
     * Only support subject with prefix, such as User:test
     */
    Subject((byte) 7, "Subject"),

    USER((byte) 8, "User");

    @JSONField(value = true)
    private final byte code;
    private final String name;

    ResourceType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static ResourceType getByName(String name) {
        for (ResourceType resourceType : ResourceType.values()) {
            if (StringUtils.equalsIgnoreCase(resourceType.getName(), name)) {
                return resourceType;
            }
        }
        return null;
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
