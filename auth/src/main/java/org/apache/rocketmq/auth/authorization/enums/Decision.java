package org.apache.rocketmq.auth.authorization.enums;

public enum Decision {

    GRANT("Grant"),

    DENY("Deny");

    private final String code;

    Decision(String code) {
        this.code = code;
    }

    public static Decision getByCode(String decision) {
        for (Decision d : Decision.values()) {
            if (d.code.equals(decision)) {
                return d;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }
}
