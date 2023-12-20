package org.apache.rocketmq.auth.authorization.enums;

import com.alibaba.fastjson2.annotation.JSONField;

public enum PolicyType {

    CUSTOM((byte) 1, "CUSTOM"),

    DEFAULT((byte) 2, "DEFAULT");

    @JSONField(value = true)
    private final byte code;
    private final String name;

    PolicyType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static PolicyType getByName(String name) {
        for (PolicyType policyType : PolicyType.values()) {
            if (policyType.getName().equals(name)) {
                return policyType;
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
