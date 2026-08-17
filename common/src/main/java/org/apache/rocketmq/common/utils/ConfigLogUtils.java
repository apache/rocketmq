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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.annotation.SensitiveConfig;

public final class ConfigLogUtils {
    public static final String REDACTED_VALUE = "******";

    private static final ConcurrentMap<Class<?>, Set<String>> SENSITIVE_PROPERTIES_BY_CLASS =
        new ConcurrentHashMap<>();

    private static final Set<String> SENSITIVE_CONFIG_KEYS = new HashSet<>(Arrays.asList(
        "initauthenticationuser",
        "innerclientauthenticationcredentials",
        "metricsgrpcexporterheader",
        "socksproxyconfig"
    ));

    private static final String[] SENSITIVE_KEYWORDS = {
        "password",
        "passwd",
        "secret",
        "credential",
        "accesskey",
        "privatekey",
        "signature",
        "apikey",
        "encryptionkey",
        "housekeepingkey",
        "authtoken",
        "secrettoken",
        "securitytoken",
        "sessiontoken"
    };

    private ConfigLogUtils() {
    }

    public static boolean isSensitiveConfigKey(String key) {
        if (key == null) {
            return false;
        }

        String normalizedKey = normalize(key);
        if (SENSITIVE_CONFIG_KEYS.contains(normalizedKey)) {
            return true;
        }
        for (String keyword : SENSITIVE_KEYWORDS) {
            if (normalizedKey.contains(keyword)) {
                return true;
            }
        }

        return hasSensitiveAbbreviationSuffix(key, "AK")
            || hasSensitiveAbbreviationSuffix(key, "SK")
            || hasSensitiveAbbreviationSuffix(key, "Token");
    }

    public static Object getValueForLog(String key, Object value) {
        return isSensitiveConfigKey(key) ? maskSensitiveValue(value) : value;
    }

    public static Object getValueForLog(Object configObject, String key, Object value) {
        if (isSensitiveConfigProperty(configObject, key) || isSensitiveConfigKey(key)) {
            return maskSensitiveValue(value);
        }
        return value;
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

    public static Properties redactSensitiveProperties(Properties properties) {
        if (properties == null) {
            return null;
        }

        Properties redacted = new Properties();
        properties.forEach((key, value) ->
            redacted.put(key, getValueForLog(String.valueOf(key), value)));
        return redacted;
    }

    private static boolean isSensitiveConfigProperty(Object configObject, String key) {
        if (configObject == null || key == null) {
            return false;
        }
        return SENSITIVE_PROPERTIES_BY_CLASS
            .computeIfAbsent(configObject.getClass(), ConfigLogUtils::findSensitiveProperties)
            .contains(key);
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

    private static String normalize(String key) {
        StringBuilder result = new StringBuilder(key.length());
        for (int i = 0; i < key.length(); i++) {
            char current = key.charAt(i);
            if (Character.isLetterOrDigit(current)) {
                result.append(Character.toLowerCase(current));
            }
        }
        return result.toString();
    }

    private static boolean hasSensitiveAbbreviationSuffix(String key, String suffix) {
        String trimmedKey = key.trim();
        String camelCaseSuffix = suffix.substring(0, 1)
            + suffix.substring(1).toLowerCase(Locale.ROOT);
        if (trimmedKey.equalsIgnoreCase(suffix)
            || trimmedKey.endsWith(suffix)
            || trimmedKey.endsWith(camelCaseSuffix)) {
            return true;
        }

        String lowerCaseKey = trimmedKey.toLowerCase(Locale.ROOT);
        String lowerCaseSuffix = suffix.toLowerCase(Locale.ROOT);
        return lowerCaseKey.endsWith("_" + lowerCaseSuffix)
            || lowerCaseKey.endsWith("-" + lowerCaseSuffix)
            || lowerCaseKey.endsWith("." + lowerCaseSuffix);
    }
}
