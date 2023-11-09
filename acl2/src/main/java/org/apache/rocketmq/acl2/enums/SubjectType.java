package org.apache.rocketmq.acl2.enums;

public enum SubjectType {

    USER("User");

    private String code;

    SubjectType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
