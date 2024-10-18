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
package org.apache.rocketmq.common.attribute;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeParser {

    public static final String ATTR_ARRAY_SEPARATOR_COMMA = ",";
    public static final String ATTR_KEY_VALUE_EQUAL_SIGN = "=";
    public static final String ATTR_ADD_PLUS_SIGN = "+";
    private static final String ATTR_DELETE_MINUS_SIGN = "-";

    public static Map<String, String> parseToMap(String attributesModification) {
        if (Strings.isNullOrEmpty(attributesModification)) {
            return new HashMap<>();
        }

        Map<String, String> attributes = new HashMap<>();
        String[] kvs = attributesModification.split(ATTR_ARRAY_SEPARATOR_COMMA);

        for (String kv : kvs) {
            String[] splits = kv.split(ATTR_KEY_VALUE_EQUAL_SIGN, 2);
            String key = splits[0];
            String value = splits.length > 1 ? splits[1] : "";

            if (key.startsWith(ATTR_ADD_PLUS_SIGN)) {
                // 添加或修改属性
                key = key.substring(1);
                if (value.isEmpty()) {
                    throw new IllegalArgumentException("Add/alter attribute requires a value: " + key);
                }
            } else if (key.startsWith(ATTR_DELETE_MINUS_SIGN)) {
                // 删除属性
                key = key.substring(1);
                value = "";
            } else {
                throw new IllegalArgumentException("Invalid attribute format: " + kv);
            }

            if (attributes.containsKey(key)) {
                throw new IllegalArgumentException("Duplicate key: " + key);
            }

            attributes.put(key, value);
        }

        return attributes;
    }

    public static String parseToString(Map<String, String> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return "";
        }

        List<String> kvs = new ArrayList<>();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (Strings.isNullOrEmpty(value)) {
                kvs.add(ATTR_DELETE_MINUS_SIGN + key);
            } else {
                kvs.add(ATTR_ADD_PLUS_SIGN + key + ATTR_KEY_VALUE_EQUAL_SIGN + value);
            }
        }

        return String.join(ATTR_ARRAY_SEPARATOR_COMMA, kvs);
    }
}
