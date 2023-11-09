package org.apache.rocketmq.acl2.enums;

public enum Decision {

    GRANT("Grant"),

    DENY("Deny");

    private final String code;

    Decision(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
