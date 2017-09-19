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

package org.apache.rocketmq.remoting.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BeanUtils {
    private final static Logger LOG = LoggerFactory.getLogger(BeanUtils.class);

    /**
     * <p>Populate the JavaBeans properties of the specified bean, based on
     * the specified name/value pairs.  This method uses Java reflection APIs
     * to identify corresponding "property setter" method names, and deals
     * with setter arguments of type <Code>String</Code>, <Code>boolean</Code>,
     * <Code>int</Code>, <Code>long</Code>, <Code>float</Code>, and
     * <Code>double</Code>.</p>
     *
     * <p>The particular setter method to be called for each property is
     * determined using the usual JavaBeans introspection mechanisms.  Thus,
     * you may identify custom setter methods using a BeanInfo class that is
     * associated with the class of the bean itself.  If no such BeanInfo
     * class is available, the standard method name conversion ("set" plus
     * the capitalized name of the property in question) is used.</p>
     *
     * <p><strong>NOTE</strong>:  It is contrary to the JavaBeans Specification
     * to have more than one setter method (with different argument
     * signatures) for the same property.</p>
     *
     * @param clazz JavaBean class whose properties are being populated
     * @param properties Map keyed by property name, with the corresponding (String or String[]) value(s) to be set
     * @param <T> Class type
     * @return Class instance
     */
    public static <T> T populate(final Properties properties, final Class<T> clazz) {
        T obj = null;
        try {
            obj = clazz.newInstance();
            return populate(properties, obj);
        } catch (Throwable e) {
            LOG.warn("Error occurs !", e);
        }
        return obj;
    }

    private static <T> void setField(final Field field, final Properties properties, final T obj) throws Exception {
        Type fieldType = field.getType();
        String fieldName = field.getName();

        String value = null;
        String configName = convertToConfigName(fieldName);
        String envName = convertToEnvName(fieldName);

        if (properties.containsKey(envName)) {
            value = properties.getProperty(envName);
        }

        if (properties.containsKey(configName)) {
            value = properties.getProperty(configName);
        }

        if (value == null) {
            return;
        }

        if (fieldType == Boolean.TYPE) {
            field.set(obj, Boolean.valueOf(value));
        } else if (fieldType == Integer.TYPE) {
            field.set(obj, Integer.valueOf(value));
        } else if (fieldType == Double.TYPE) {
            field.set(obj, Double.valueOf(value));
        } else if (fieldType == Float.TYPE) {
            field.set(obj, Float.valueOf(value));
        } else if (fieldType == Long.TYPE) {
            field.set(obj, Long.valueOf(value));
        } else
            field.set(obj, value);
    }

    private static String convertToConfigName(String variableName) {
        StringBuilder sb = new StringBuilder();
        for (char c : variableName.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append('.');
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    private static String convertToEnvName(String variableName) {
        StringBuilder sb = new StringBuilder();
        for (char c : variableName.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append('_');
            }
            sb.append(Character.toUpperCase(c));
        }
        return sb.toString();
    }

    public static <T> T populate(final Properties properties, final T obj) {
        Class<?> clazz = obj.getClass();
        List<Field> allFields = new ArrayList<>();
        allFields = getAllFields(allFields, clazz);
        Properties fullProp = extractProperties(properties);

        try {
            for (Field field : allFields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    setField(field, fullProp, obj);
                }
            }
        } catch (Exception e) {
            LOG.warn("Error occurs !", e);
        }
        return obj;
    }

    public static String configObjectToString(final Object object) {
        List<Field> allFields = new ArrayList<>();
        getAllFields(allFields, object.getClass());
        StringBuilder sb = new StringBuilder();
        for (Field field : allFields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                        if (null == value) {
                            value = "";
                        }
                    } catch (IllegalAccessException ignored) {
                    }
                    sb.append(name).append("=").append(value).append("%n");
                }
            }
        }
        return sb.toString();
    }

    private static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.asList(type.getDeclaredFields()));
        if (type.getSuperclass() != null) {
            getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }

    private static Properties extractProperties(final Properties properties) {
        Properties newPro = new Properties();

        Map<String, String> envMap = System.getenv();
        for (final Map.Entry<String, String> entry : envMap.entrySet()) {
            newPro.setProperty(entry.getKey().toUpperCase(), entry.getValue());
            newPro.setProperty(entry.getKey().toLowerCase(), entry.getValue()); //EnvProp supports A_B_C and a.b.c
        }

        Properties systemProp = System.getProperties(); //SystemProp supports a.b.c
        for (final Map.Entry<Object, Object> entry : systemProp.entrySet()) {
            newPro.setProperty(String.valueOf(entry.getKey()).toLowerCase(), String.valueOf(entry.getValue()));
        }

        Properties inner = null;
        try {
            Field field = Properties.class.getDeclaredField("defaults");
            field.setAccessible(true);
            inner = (Properties) field.get(properties);
        } catch (Exception ignore) {
        }

        if (inner != null) {
            for (final Map.Entry<Object, Object> entry : inner.entrySet()) {
                newPro.setProperty(String.valueOf(entry.getKey()).toLowerCase(), String.valueOf(entry.getValue()));
            }
        }

        for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
            newPro.setProperty(String.valueOf(entry.getKey()).toLowerCase(), String.valueOf(entry.getValue()));
        }

        return newPro;
    }
}
