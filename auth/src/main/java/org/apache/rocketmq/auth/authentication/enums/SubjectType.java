package org.apache.rocketmq.auth.authentication.enums;

public enum SubjectType {

    USER("User");

    private String code;

    SubjectType(String code) {
        this.code = code;
    }

    public static SubjectType getByCode(String code) {
        for (SubjectType subjectType : SubjectType.values()) {
            if (subjectType.getCode().equals(code)) {
                return subjectType;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }
}
