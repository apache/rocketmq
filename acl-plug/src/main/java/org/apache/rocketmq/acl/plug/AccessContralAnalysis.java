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
package org.apache.rocketmq.acl.plug;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.exception.AclPlugRuntimeException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class AccessContralAnalysis {

    private Map<Class<?>, Map<Integer, Field>> classTocodeAndMentod = new HashMap<>();

    private Map<String, Integer> fieldNameAndCode = new HashMap<>();

    public void analysisClass(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        try {
            for (Field field : fields) {
                if (field.getType().equals(int.class)) {
                    String name = StringUtils.replace(field.getName(), "_", "").toLowerCase();
                    fieldNameAndCode.put(name, (Integer) field.get(null));
                }

            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new AclPlugRuntimeException(String.format("analysis on failure Class is %s", clazz.getName()), e);
        }
    }

    public Map<Integer, Boolean> analysis(AccessControl accessControl) {
        Class<? extends AccessControl> clazz = accessControl.getClass();
        Map<Integer, Field> codeAndField = classTocodeAndMentod.get(clazz);
        if (codeAndField == null) {
            codeAndField = new HashMap<>();
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (!field.getType().equals(boolean.class))
                    continue;
                Integer code = fieldNameAndCode.get(field.getName().toLowerCase());
                if (code == null) {
                    throw new AclPlugRuntimeException(String.format("field nonexistent in code  fieldName is %s", field.getName()));
                }
                field.setAccessible(true);
                codeAndField.put(code, field);

            }
            if (codeAndField.isEmpty()) {
                throw new AclPlugRuntimeException(String.format("AccessControl nonexistent code , name %s", accessControl.getClass().getName()));
            }
            classTocodeAndMentod.put(clazz, codeAndField);
        }
        Iterator<Entry<Integer, Field>> it = codeAndField.entrySet().iterator();
        Map<Integer, Boolean> authority = new HashMap<>();
        try {
            while (it.hasNext()) {
                Entry<Integer, Field> e = it.next();
                authority.put(e.getKey(), (Boolean) e.getValue().get(accessControl));
            }
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new AclPlugRuntimeException(String.format("analysis on failure AccessControl is %s", AccessControl.class.getName()), e);
        }
        return authority;
    }

}
