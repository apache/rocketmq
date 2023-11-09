package org.apache.rocketmq.acl2.enums;

public enum Action {

    ALL("All"),

    PUB("Pub"),

    SUB("Sub"),

    CREATE("Create"),

    UPDATE("Update"),

    DELETE("Delete"),

    DESCRIBE("Describe"),

    LIST("List"),
    ;

    private final String code;

    Action(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
