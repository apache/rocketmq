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
package io.openmessaging.rocketmq.utils;

import io.openmessaging.KeyValue;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.log.ClientLogger;
import org.slf4j.Logger;

public final class BeanUtils {
    final static Logger log = ClientLogger.getLog();

    /**
     * Maps primitive {@code Class}es to their corresponding wrapper {@code Class}.
     */
    private static Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<Class<?>, Class<?>>();

    static {
        primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
        primitiveWrapperMap.put(Byte.TYPE, Byte.class);
        primitiveWrapperMap.put(Character.TYPE, Character.class);
        primitiveWrapperMap.put(Short.TYPE, Short.class);
        primitiveWrapperMap.put(Integer.TYPE, Integer.class);
        primitiveWrapperMap.put(Long.TYPE, Long.class);
        primitiveWrapperMap.put(Double.TYPE, Double.class);
        primitiveWrapperMap.put(Float.TYPE, Float.class);
        primitiveWrapperMap.put(Void.TYPE, Void.TYPE);
    }

    private static Map<Class<?>, Class<?>> wrapperMap = new HashMap<Class<?>, Class<?>>();

    static {
        for (final Class<?> primitiveClass : primitiveWrapperMap.keySet()) {
            final Class<?> wrapperClass = primitiveWrapperMap.get(primitiveClass);
            if (!primitiveClass.equals(wrapperClass)) {
                wrapperMap.put(wrapperClass, primitiveClass);
            }
        }
        wrapperMap.put(String.class, String.class);
    }

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
            log.warn("Error occurs !", e);
        }
        return obj;
    }

    public static <T> T populate(final KeyValue properties, final Class<T> clazz) {
        T obj = null;
        try {
            obj = clazz.newInstance();
            return populate(properties, obj);
        } catch (Throwable e) {
            log.warn("Error occurs !", e);
        }
        return obj;
    }

    public static Class<?> getMethodClass(Class<?> clazz, String methodName) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.getName().equalsIgnoreCase(methodName)) {
                return method.getParameterTypes()[0];
            }
        }
        return null;
    }

    public static void setProperties(Class<?> clazz, Object obj, String methodName,
        Object value) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> parameterClass = getMethodClass(clazz, methodName);
        Method setterMethod = clazz.getMethod(methodName, parameterClass);
        if (parameterClass == Boolean.TYPE) {
            setterMethod.invoke(obj, Boolean.valueOf(value.toString()));
        } else if (parameterClass == Integer.TYPE) {
            setterMethod.invoke(obj, Integer.valueOf(value.toString()));
        } else if (parameterClass == Double.TYPE) {
            setterMethod.invoke(obj, Double.valueOf(value.toString()));
        } else if (parameterClass == Float.TYPE) {
            setterMethod.invoke(obj, Float.valueOf(value.toString()));
        } else if (parameterClass == Long.TYPE) {
            setterMethod.invoke(obj, Long.valueOf(value.toString()));
        } else
            setterMethod.invoke(obj, value);
    }

    public static <T> T populate(final Properties properties, final T obj) {
        Class<?> clazz = obj.getClass();
        try {

            Set<Map.Entry<Object, Object>> entries = properties.entrySet();
            for (Map.Entry<Object, Object> entry : entries) {
                String entryKey = entry.getKey().toString();
                String[] keyGroup = entryKey.split("\\.");
                for (int i = 0; i < keyGroup.length; i++) {
                    keyGroup[i] = keyGroup[i].toLowerCase();
                    keyGroup[i] = StringUtils.capitalize(keyGroup[i]);
                }
                String beanFieldNameWithCapitalization = StringUtils.join(keyGroup);
                try {
                    setProperties(clazz, obj, "set" + beanFieldNameWithCapitalization, entry.getValue());
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
                    //ignored...
                }
            }
        } catch (RuntimeException e) {
            log.warn("Error occurs !", e);
        }
        return obj;
    }

    public static <T> T populate(final KeyValue properties, final T obj) {
        Class<?> clazz = obj.getClass();
        try {

            final Set<String> keySet = properties.keySet();
            for (String key : keySet) {
                String[] keyGroup = key.split("\\.");
                for (int i = 0; i < keyGroup.length; i++) {
                    keyGroup[i] = keyGroup[i].toLowerCase();
                    keyGroup[i] = StringUtils.capitalize(keyGroup[i]);
                }
                String beanFieldNameWithCapitalization = StringUtils.join(keyGroup);
                try {
                    setProperties(clazz, obj, "set" + beanFieldNameWithCapitalization, properties.getString(key));
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
                    //ignored...
                }
            }
        } catch (RuntimeException e) {
            log.warn("Error occurs !", e);
        }
        return obj;
    }
}

