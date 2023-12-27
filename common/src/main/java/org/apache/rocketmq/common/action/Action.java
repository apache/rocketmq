package org.apache.rocketmq.common.action;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

public enum Action {

    UNKNOWN((byte) 0, "Unknown"),

    ALL((byte) 1, "All"),

    ANY((byte) 2, "Any"),

    PUB((byte) 3, "Pub"),

    SUB((byte) 4, "Sub"),

    CREATE((byte) 5, "Create"),

    UPDATE((byte) 6, "Update"),

    DELETE((byte) 7, "Delete"),

    GET((byte) 8, "Get"),

    LIST((byte) 9, "List");

    @JSONField(value = true)
    private final byte code;
    private final String name;

    Action(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static Action getByName(String name) {
        for (Action action : Action.values()) {
            if (StringUtils.equalsIgnoreCase(action.getName(), name)) {
                return action;
            }
        }
        return null;
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
