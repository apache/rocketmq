package org.apache.rocketmq.auth.authorization.enums;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;

public enum Decision {

    GRANT((byte) 1, "Grant"),

    DENY((byte) 2, "Deny");

    @JSONField(value = true)
    private final byte code;
    private final String name;

    Decision(byte code, String name) {
        this.code = code;
        this.name = name;
    }

    public static Decision getByName(String name) {
        for (Decision decision : Decision.values()) {
            if (StringUtils.equalsIgnoreCase(decision.getName(), name)) {
                return decision;
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
