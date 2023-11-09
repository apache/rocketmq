package org.apache.rocketmq.common.action;

public enum Action {

    UNKNOWN("Unknown"),

    ALL("All"),

    ANY("Any"),

    PUB("Pub"),

    SUB("Sub"),

    CREATE("Create"),

    UPDATE("Update"),

    DELETE("Delete"),

    GET("Get"),

    LIST("List");

    private final String code;

    Action(String code) {
        this.code = code;
    }

    public static Action getByCode(String code) {
        for (Action action : Action.values()) {
            if (action.code.equals(code)) {
                return action;
            }
        }
        return null;
    }

    public String getCode() {
        return code;
    }
}
