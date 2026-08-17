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

package org.apache.rocketmq.common.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.annotation.SensitiveConfig;

/**
 * Builds logging-only configuration projections. Masking is controlled exclusively by
 * {@link SensitiveConfig}; property names are never used to infer sensitivity.
 */
public final class ConfigLogUtils {
    public static final String REDACTED_VALUE = "******";

    private static final ConcurrentMap<Class<?>, Set<String>> SENSITIVE_PROPERTIES_BY_CLASS =
        new ConcurrentHashMap<>();

    private ConfigLogUtils() {
    }

    public static Object getValueForLog(Object configObject, String key, Object value) {
        return getValueForLog(getSensitiveConfigProperties(configObject), key, value);
    }

    public static Object getValueForLog(Set<String> sensitiveConfigProperties, String key, Object value) {
        return sensitiveConfigProperties != null && sensitiveConfigProperties.contains(key)
            ? maskSensitiveValue(value) : value;
    }

    public static Object maskSensitiveValue(Object value) {
        if (value == null) {
            return null;
        }

        String text = String.valueOf(value);
        if (text.isEmpty()) {
            return text;
        }
        if (text.length() <= 4) {
            return REDACTED_VALUE;
        }
        if (text.length() <= 7) {
            return text.substring(0, 1) + REDACTED_VALUE + text.substring(text.length() - 1);
        }
        return text.substring(0, 2) + REDACTED_VALUE + text.substring(text.length() - 2);
    }

    public static Properties redactSensitiveProperties(Properties properties,
        Set<String> sensitiveConfigProperties) {
        if (properties == null) {
            return null;
        }

        Properties redacted = new Properties();
        properties.forEach((key, value) ->
            redacted.put(key, getValueForLog(sensitiveConfigProperties, String.valueOf(key), value)));
        return redacted;
    }

    public static Set<String> getSensitiveConfigProperties(Object configObject) {
        if (configObject == null) {
            return Collections.emptySet();
        }
        return SENSITIVE_PROPERTIES_BY_CLASS
            .computeIfAbsent(configObject.getClass(), ConfigLogUtils::findSensitiveProperties);
    }

    private static Set<String> findSensitiveProperties(Class<?> configClass) {
        Set<String> properties = new HashSet<>();
        for (Class<?> current = configClass; current != null && current != Object.class;
            current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                if (field.isAnnotationPresent(SensitiveConfig.class)) {
                    properties.add(field.getName());
                }
            }
            for (Method method : current.getDeclaredMethods()) {
                if (method.isAnnotationPresent(SensitiveConfig.class)) {
                    String propertyName = propertyName(method.getName());
                    if (propertyName != null) {
                        properties.add(propertyName);
                    }
                }
            }
        }
        return Collections.unmodifiableSet(properties);
    }

    private static String propertyName(String methodName) {
        String name = null;
        if (methodName.startsWith("get") || methodName.startsWith("set")) {
            name = methodName.substring(3);
        } else if (methodName.startsWith("is")) {
            name = methodName.substring(2);
        }
        if (name == null || name.isEmpty()) {
            return null;
        }
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
}
