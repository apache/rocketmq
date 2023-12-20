package org.apache.rocketmq.auth.authentication.enums;

import com.alibaba.fastjson2.annotation.JSONField;

public enum SubjectType {

    USER((byte) 1, "User");

    @JSONField(value = true)
    private final byte code;
    private final String name;

    SubjectType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static SubjectType getByName(String name) {
        for (SubjectType subjectType : SubjectType.values()) {
            if (subjectType.getName().equals(name)) {
                return subjectType;
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
