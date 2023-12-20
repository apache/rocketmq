package org.apache.rocketmq.common.resource;

import com.alibaba.fastjson2.annotation.JSONField;

public enum ResourcePattern {

    ANY((byte) 1, "ANY"),

    LITERAL((byte) 2, "LITERAL"),

    PREFIXED((byte) 3, "PREFIXED");

    @JSONField(value = true)
    private final byte code;
    private final String name;

    ResourcePattern(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
