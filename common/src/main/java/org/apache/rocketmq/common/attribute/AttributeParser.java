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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeParser {
    public static Map<String, String> parseToMap(String attributesModification) {
        if (Strings.isNullOrEmpty(attributesModification)) {
            return new HashMap<>();
        }

        // format: +key1=value1,+key2=value2,-key3,+key4=value4
        Map<String, String> attributes = new HashMap<>();
        String arraySeparator = ",";
        String kvSeparator = "=";
        String[] kvs = attributesModification.split(arraySeparator);
        for (String kv : kvs) {
            String key;
            String value;
            if (kv.contains(kvSeparator)) {
                key = kv.split(kvSeparator)[0];
                value = kv.split(kvSeparator)[1];
                if (!key.contains("+")) {
                    throw new RuntimeException("add/alter attribute format is wrong: " + key);
                }
            } else {
                key = kv;
                value = "";
                if (!key.contains("-")) {
                    throw new RuntimeException("delete attribute format is wrong: " + key);
                }
            }
            String old = attributes.put(key, value);
            if (old != null) {
                throw new RuntimeException("key duplication: " + key);
            }
        }
        return attributes;
    }

    public static String parseToString(Map<String, String> attributes) {
        if (attributes == null || attributes.size() == 0) {
            return "";
        }

        List<String> kvs = new ArrayList<>();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {

            String value = entry.getValue();
            if (Strings.isNullOrEmpty(value)) {
                kvs.add(entry.getKey());
            } else {
                kvs.add(entry.getKey() + "=" + entry.getValue());
            }
        }
        return Joiner.on(",").join(kvs);
    }
}
