package org.apache.rocketmq.acl2.enums;

public enum ResourcePattern {
    LITERAL("LITERAL"),

    PREFIXED("PREFIXED");

    private final String code;

    ResourcePattern(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
