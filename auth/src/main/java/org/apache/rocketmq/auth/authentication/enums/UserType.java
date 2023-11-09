package org.apache.rocketmq.auth.authentication.enums;

import java.util.Arrays;

public enum UserType {

    SUPER((byte) 1, "Super"),

    CUSTOM((byte) 2, "Custom");

    private final byte code;

    private final String name;

    UserType(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static UserType getByName(String name) {
        for (UserType subjectType : UserType.values()) {
            if (subjectType.getName().equals(name)) {
                return subjectType;
            }
        }
        return null;
    }

    public static UserType getByCode(byte code) {
        return Arrays.stream(UserType.values())
            .filter(type -> type.getCode() == code)
            .findAny().orElse(null);
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
