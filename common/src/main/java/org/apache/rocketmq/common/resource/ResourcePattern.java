package org.apache.rocketmq.common.resource;

public enum ResourcePattern {

    ANY("ANY"),

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
