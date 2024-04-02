/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
