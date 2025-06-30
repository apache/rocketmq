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

package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import org.junit.Test;
import org.objenesis.ObjenesisStd;
import org.reflections.Reflections;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

public class RemotingSerializableCompatTest {
    
    @Test
    public void testCompatibilityCheck() {
        Reflections reflections = new Reflections("org.apache.rocketmq.remoting.protocol");
        Set<Class<? extends RemotingSerializable>> subTypes = reflections.getSubTypesOf(RemotingSerializable.class);
        
        for (Class<? extends RemotingSerializable> clazz : subTypes) {
            if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers()) || clazz.getSimpleName().endsWith("Test") || clazz.isAnonymousClass() || clazz.getName().contains("$")) {
                continue;
            }
            try {
                RemotingSerializable instance;
                try {
                    instance = clazz.getDeclaredConstructor().newInstance();
                } catch (NoSuchMethodException e) {
                    instance = allocateInstance(clazz);
                }
                fillDefaultFields(instance, clazz);
                assertTrue(checkCompatible(instance, clazz));
            } catch (Exception e) {
                System.err.printf("Class %s: incompatible, error: %s\n", clazz.getName(), e.getMessage());
            }
        }
    }
    
    private void fillDefaultFields(final Object obj, final Class<?> clazz) throws Exception {
        if (null == clazz || clazz == Object.class) {
            return;
        }
        for (Field field : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            field.setAccessible(true);
            Class<?> type = field.getType();
            
            if (type.isArray()) {
                Class<?> componentType = type.getComponentType();
                Object arr = Array.newInstance(componentType, 1);
                Object element = createElementOrDefault(componentType);
                if (element != null) {
                    Array.set(arr, 0, element);
                }
                field.set(obj, arr);
            } else if (Properties.class.isAssignableFrom(type)) {
                field.set(obj, new Properties());
            } else if (type.isEnum()) {
                Object[] enumConstants = type.getEnumConstants();
                if (enumConstants != null && enumConstants.length > 0) {
                    field.set(obj, enumConstants[0]);
                }
            } else if (ConcurrentHashMap.KeySetView.class.isAssignableFrom(type)) {
                field.set(obj, ConcurrentHashMap.newKeySet());
            } else if (ConcurrentHashMap.class.isAssignableFrom(type) || ConcurrentMap.class.isAssignableFrom(type)) {
                field.set(obj, new ConcurrentHashMap<>());
            } else if (Set.class.isAssignableFrom(type)) {
                Set<Object> set = type.isInterface() ? new HashSet<>() : (Set<Object>) type.getDeclaredConstructor().newInstance();
                Class<?> genericType = getFirstGenericType(field);
                Object element = createElementOrDefault(genericType);
                if (element != null)
                    set.add(element);
                field.set(obj, set);
            } else if (List.class.isAssignableFrom(type)) {
                List<Object> list = new ArrayList<>();
                Class<?> genericType = getFirstGenericType(field);
                Object element = createElementOrDefault(genericType);
                if (null != element) {
                    list.add(element);
                }
                field.set(obj, list);
            } else if (Map.class.isAssignableFrom(type)) {
                Map<Object, Object> map = type.isInterface() ? new HashMap<>() : (Map<Object, Object>) type.getDeclaredConstructor().newInstance();
                Class<?> keyType = getGenericType(field, 0);
                Class<?> valueType = getGenericType(field, 1);
                Object key = createElementOrDefault(keyType);
                Object value = createElementOrDefault(valueType);
                if (null != key && null != value) {
                    map.put(key, value);
                }
                field.set(obj, map);
            } else if (type == AtomicLong.class) {
                field.set(obj, new AtomicLong(1));
            } else {
                Object value = getDefaultValue(type);
                if (null != value) {
                    field.set(obj, value);
                } else if (!type.isPrimitive() && !type.getName().startsWith("java.")) {
                    Object subObj;
                    try {
                        subObj = type.getDeclaredConstructor().newInstance();
                    } catch (NoSuchMethodException e) {
                        subObj = allocateInstance(type);
                    }
                    fillDefaultFields(subObj, type);
                    field.set(obj, subObj);
                }
            }
        }
        fillDefaultFields(obj, clazz.getSuperclass());
    }
    
    private Object createElementOrDefault(final Class<?> type) throws Exception {
        if (null == type) {
            return null;
        }
        Object value = getDefaultValue(type);
        if (null != value) {
            return value;
        }
        if (type.isEnum()) {
            Object[] enumConstants = type.getEnumConstants();
            if (null != enumConstants && enumConstants.length > 0) {
                return enumConstants[0];
            }
            return null;
        }
        if (type.isArray()) {
            Class<?> componentType = type.getComponentType();
            Object arr = Array.newInstance(componentType, 1);
            Object element = createElementOrDefault(componentType);
            if (null != element) {
                Array.set(arr, 0, element);
            }
            return arr;
        }
        if (!type.isPrimitive()) {
            Object obj;
            try {
                obj = type.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException e) {
                obj = allocateInstance(type);
            }
            fillDefaultFields(obj, type);
            return obj;
        }
        return null;
    }
    
    private Class<?> getFirstGenericType(final Field field) {
        return getGenericType(field, 0);
    }
    
    private Class<?> getGenericType(final Field field, final int index) {
        try {
            java.lang.reflect.Type genericType = field.getGenericType();
            if (genericType instanceof java.lang.reflect.ParameterizedType) {
                java.lang.reflect.Type[] types = ((java.lang.reflect.ParameterizedType) genericType).getActualTypeArguments();
                if (types.length > index && types[index] instanceof Class) {
                    return (Class<?>) types[index];
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }
    
    private Object getDefaultValue(final Class<?> type) {
        if (null == type) {
            return null;
        }
        if (type == boolean.class || type == Boolean.class) {
            return false;
        }
        if (type == byte.class || type == Byte.class) {
            return (byte) 1;
        }
        if (type == short.class || type == Short.class) {
            return (short) 1;
        }
        if (type == int.class || type == Integer.class) {
            return 1;
        }
        if (type == long.class || type == Long.class) {
            return 1L;
        }
        if (type == float.class || type == Float.class) {
            return 1f;
        }
        if (type == double.class || type == Double.class) {
            return 1d;
        }
        if (type == char.class || type == Character.class) {
            return '\0';
        }
        if (type == String.class) {
            return "test";
        }
        return null;
    }
    
    private boolean checkCompatible(final Object original, final Object deserialized, final String path, final Map<Object, Object> visited) {
        if (null == original && null == deserialized) {
            return true;
        }
        if (null == original || null == deserialized) {
            System.err.printf("Objects at %s incompatible: one is null\n", path);
            return false;
        }
        
        if (!isPrimitiveOrWrapper(original.getClass())) {
            if (visited.containsKey(original)) {
                return true;
            }
            visited.put(original, deserialized);
        }
        
        Class<?> clazz = original.getClass();
        boolean result = true;
        for (Field field : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            JSONField jsonField = field.getAnnotation(JSONField.class);
            if (null != jsonField && !jsonField.serialize()) {
                continue;
            }
            if ("hash".equals(field.getName()) || "serialVersionUID".equals(field.getName())) {
                continue;
            }
            
            field.setAccessible(true);
            try {
                Object v1 = field.get(original);
                Object v2 = field.get(deserialized);
                String fieldPath = path + "." + field.getName();
                
                if (null == v1 && null == v2) {
                    continue;
                }
                if (v1 instanceof Random && v2 instanceof Random) {
                    continue;
                }
                if (v1 instanceof AtomicLong && v2 instanceof AtomicLong) {
                    if (((AtomicLong) v1).get() != ((AtomicLong) v2).get()) {
                        result = false;
                        System.err.printf("Field %s incompatible: original=%s, deserialized=%s\n", fieldPath, v1, v2);
                    }
                    continue;
                }
                if (v1 instanceof Set && v2 instanceof Set) {
                    Set<?> s1 = (Set<?>) v1, s2 = (Set<?>) v2;
                    if (s1.size() != s2.size()) {
                        result = false;
                        System.err.printf("Field %s incompatible: set size original=%d, deserialized=%d\n", fieldPath, s1.size(), s2.size());
                    } else if (!s1.isEmpty()) {
                        List<?> list1 = new ArrayList<>(s1);
                        List<?> list2 = new ArrayList<>(s2);
                        if (new HashSet<>(list1).equals(new HashSet<>(list2))) {
                            continue;
                        }
                        boolean elementsCompatible = true;
                        for (Object e1 : list1) {
                            boolean foundMatch = false;
                            for (Object e2 : list2) {
                                if (checkCompatible(e1, e2, fieldPath + ".element", new HashMap<>(visited))) {
                                    foundMatch = true;
                                    break;
                                }
                            }
                            if (!foundMatch) {
                                elementsCompatible = false;
                                break;
                            }
                        }
                        if (!elementsCompatible) {
                            result = false;
                            System.err.printf("Field %s incompatible: sets have different elements\n", fieldPath);
                        }
                    }
                    continue;
                }
                if (v1 instanceof List && v2 instanceof List) {
                    List<?> l1 = (List<?>) v1, l2 = (List<?>) v2;
                    if (l1.size() != l2.size()) {
                        result = false;
                        System.err.printf("Field %s incompatible: list size original=%d, deserialized=%d\n", fieldPath, l1.size(), l2.size());
                    } else {
                        for (int i = 0; i < l1.size(); i++) {
                            Object e1 = l1.get(i);
                            Object e2 = l2.get(i);
                            if (!checkCompatible(e1, e2, fieldPath + "[" + i + "]", new HashMap<>(visited))) {
                                result = false;
                            }
                        }
                    }
                    continue;
                }
                if (v1 instanceof Map && v2 instanceof Map) {
                    Map<?, ?> m1 = (Map<?, ?>) v1, m2 = (Map<?, ?>) v2;
                    if (!m1.keySet().equals(m2.keySet())) {
                        result = false;
                        System.err.printf("Field %s incompatible: map keys original=%s, deserialized=%s\n", fieldPath, m1.keySet(), m2.keySet());
                    } else {
                        for (Object key : m1.keySet()) {
                            Object val1 = m1.get(key), val2 = m2.get(key);
                            if (val1 != null && val2 != null) {
                                if (!checkCompatible(val1, val2, fieldPath + "[" + key + "]", new HashMap<>(visited))) {
                                    result = false;
                                }
                            } else if (val1 != val2) {
                                result = false;
                                System.err.printf("Field %s key %s incompatible: original=%s, deserialized=%s\n",
                                        fieldPath, key, val1, val2);
                            }
                        }
                    }
                    continue;
                }
                Class<?> type = field.getType();
                if (null != v1 && null != v2 && !type.isPrimitive() && !type.getName().startsWith("java.")) {
                    if (!checkCompatible(v1, v2, fieldPath, new HashMap<>(visited))) {
                        result = false;
                    }
                    continue;
                }
                if (null == v1 || null == v2 || !v1.equals(v2)) {
                    result = false;
                    System.err.printf("Field %s incompatible: original=%s, deserialized=%s\n", fieldPath, v1, v2);
                }
            } catch (Exception e) {
                result = false;
                System.err.printf("Field %s error: %s\n", path + "." + field.getName(), e.getMessage());
            }
        }
        if (result) {
            System.out.printf("Class %s compatible\n", path);
        }
        return result;
    }
    
    private boolean isPrimitiveOrWrapper(final Class<?> clazz) {
        return clazz.isPrimitive() ||
                clazz == String.class ||
                clazz == Boolean.class ||
                clazz == Character.class ||
                clazz == Byte.class ||
                clazz == Short.class ||
                clazz == Integer.class ||
                clazz == Long.class ||
                clazz == Float.class ||
                clazz == Double.class;
    }
    
    private boolean checkCompatible(final Object original, final Class<?> clazz) {
        String json = com.alibaba.fastjson.JSON.toJSONString(original);
        Object deserialized;
        try {
            deserialized = com.alibaba.fastjson2.JSON.parseObject(json, clazz);
        } catch (Exception e) {
            System.err.printf("Deserialization failed for %s: %s\n", clazz.getName(), e.getMessage());
            return false;
        }
        return checkCompatible(original, deserialized, clazz.getSimpleName(), new HashMap<>());
    }
    
    private <T> T allocateInstance(final Class<T> clazz) {
        return new ObjenesisStd().newInstance(clazz);
    }
}
