package org.apache.rocketmq.auth.authorization.enums;

public enum PolicyType {

    CUSTOM("CUSTOM"),

    DEFAULT("DEFAULT");

    private String code;

    PolicyType(String code) {
        this.code = code;
    }

    public static PolicyType getByCode(String type) {
        for (PolicyType policyType : PolicyType.values()) {
            if (policyType.getCode().equals(type)) {
                return policyType;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }
}
