package org.apache.rocketmq.common.action;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

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

    // 添加静态Map用于存储名称到枚举值的映射
    private static final Map<String, Action> NAME_MAP = new HashMap<>();

    static {
        // 初始化Map
        for (Action action : values()) {
            NAME_MAP.put(action.getName().toLowerCase(), action);
        }
    }

    Action(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static Action getByName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        return NAME_MAP.get(name.toLowerCase());
    }

    public byte getCode() {
        return code;
    }

    public String getName() {
        return name;
    }
}
