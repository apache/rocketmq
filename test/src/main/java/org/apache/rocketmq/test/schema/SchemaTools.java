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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.test.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.rocketmq.test.schema.SchemaDefiner.FIELD_CLASS_NAMES;
import static org.apache.rocketmq.test.schema.SchemaDefiner.IGNORED_FIELDS;

public class SchemaTools {
    public static final String PATH_API = "api";
    public static final String PATH_PROTOCOL = "protocol";

    public static String isPublicOrPrivate(int modifiers) {
        if (Modifier.isPublic(modifiers)) {
            return "public";
        } else if (Modifier.isProtected(modifiers)) {
            return "protected";
        } else {
            return "private";
        }
    }
    public static TreeMap<String, String> buildSchemaOfFields(Class apiClass) throws Exception {
        List<Field> fields = new ArrayList<>();
        Class current = apiClass;
        do {
            fields.addAll(Arrays.asList(current.getDeclaredFields()));
        } while ((current = current.getSuperclass()) != null
            && current != Object.class);
        Object obj = null;
        if (!apiClass.isInterface()
            && !Modifier.isAbstract(apiClass.getModifiers())
            && !apiClass.isEnum()) {
            Constructor<?> constructor = null;
            for (Constructor<?> tmp : apiClass.getConstructors()) {
                if (constructor == null) {
                    constructor = tmp;
                }
                if (tmp.getParameterCount() < constructor.getParameterCount()) {
                    constructor = tmp;
                }
            }
            assert constructor != null;
            constructor.setAccessible(true);
            final String msg = constructor.getName();
            try {
                obj = constructor.newInstance(Arrays.stream(constructor.getParameterTypes()).map(x -> {
                    try {
                        if (x.isEnum()) {
                            return x.getEnumConstants()[0];
                        } if (x == boolean.class) {
                            return false;
                        } else if (x == char.class) {
                            return "";
                        } else if (x.isPrimitive()) {
                            return 0;
                        } else {
                            return x.newInstance();
                        }
                    } catch (InstantiationException instantiationException) {
                        return x.cast(null);
                    } catch (Exception e) {
                        throw new RuntimeException(msg + " " + x.getName(), e);
                    }
                }).toArray());
            } catch (Exception e) {
                throw new RuntimeException(msg, e);
            }
        }
        TreeMap<String, String> map = new TreeMap<>();
        if (apiClass.isEnum()) {
            for (Object enumObject: apiClass.getEnumConstants()) {
                String name = ((Enum<?>)enumObject).name();
                int ordinal = ((Enum<?>)enumObject).ordinal();
                String key = String.format("Field %s", name);
                String value = String.format("%s %s %s", "public", "int", "" + ordinal);
                map.put(key, value);
            }
            return map;
        }
        for (Field field: fields) {
            if (field.getName().startsWith("$")) {
                //inner fields
                continue;
            }
            String key = String.format("Field %s", field.getName());
            boolean ignore = false;
            for (Class<?> tmpClass: IGNORED_FIELDS.keySet()) {
                if (tmpClass.isAssignableFrom(apiClass)
                    && IGNORED_FIELDS.get(tmpClass).contains(field.getName())) {
                    ignore = true;
                    //System.out.printf("Ignore AAA:%s %s %s\n", apiClass.getName(), field.getName(), field.getType().getName());
                    break;
                }
            }
            if (!field.getType().isEnum()
                && !field.getType().isPrimitive()
                && !FIELD_CLASS_NAMES.contains(field.getType().getName())) {
                //System.out.printf("Ignore BBB:%s %s %s\n", apiClass.getName(), field.getName(), field.getType().getName());
                ignore = true;
            }
            field.setAccessible(true);
            Object fieldValue = "null";
            try {
                fieldValue = field.get(obj);
            } catch (Exception e) {
                throw new RuntimeException(apiClass.getName() + " " + field.getName(), e);
            }
            if (ignore) {
                //System.out.printf("Ignore:%s %s %s\n", apiClass.getName(), field.getName(), field.getType().getName());
                fieldValue = "null";
            }
            String value = String.format("%s %s %s", isPublicOrPrivate(field.getModifiers()), field.getType().getName(), fieldValue);
            map.put(key, value);
        }
        return map;
    }

    public static TreeMap<String, String> buildSchemaOfMethods(Class apiClass) throws Exception {
        List<Method> methods = new ArrayList<>();
        Class current = apiClass;
        do {
            methods.addAll(Arrays.asList(current.getDeclaredMethods()));
        } while ((current = current.getSuperclass()) != null
            && current != Object.class);
        TreeMap<String, String> map = new TreeMap<>();
        if (apiClass.isEnum()) {
            return map;
        }
        for (Method method: methods) {
            if (!Modifier.isPublic(method.getModifiers())) {
                //only care for the public methods
                continue;
            }
            Class<?>[] parameterTypes = method.getParameterTypes();
            Arrays.sort(parameterTypes, new Comparator<Class<?>>() {
                @Override
                public int compare(Class<?> o1, Class<?> o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            Class<?>[] exceptionTypes = method.getExceptionTypes();
            Arrays.sort(exceptionTypes, new Comparator<Class<?>>() {
                @Override
                public int compare(Class<?> o1, Class<?> o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            String key = String.format("Method %s(%s)", method.getName(), Arrays.stream(parameterTypes).map(Class::getName).collect(Collectors.joining(",")));
            String value = String.format("%s throws (%s): %s",
                isPublicOrPrivate(method.getModifiers()),
                method.getReturnType().getName(),
                Arrays.stream(exceptionTypes).map(Class::getName).collect(Collectors.joining(",")));
            map.put(key, value);
        }
        return map;
    }


    public static Map<String, TreeMap<String, String>> generate(List<Class<?>> classList) throws Exception {
        Map<String, TreeMap<String, String>> schemaMap = new HashMap<>();
        for (Class<?> apiClass : classList) {
            TreeMap<String, String> map = new TreeMap<>();
            map.putAll(buildSchemaOfFields(apiClass));
            map.putAll(buildSchemaOfMethods(apiClass));
            schemaMap.put(apiClass.getName().replace("org.apache.rocketmq.", ""), map);
        }
        return schemaMap;
    }

    public static void write(Map<String, TreeMap<String, String>> schemaMap, String base, String label) throws Exception {
        for (Map.Entry<String, TreeMap<String, String>> entry : schemaMap.entrySet()) {
            TreeMap<String, String> map = entry.getValue();
            final String fileName = String.format("%s/%s/%s.schema", base, label, entry.getKey());
            File file = new File(fileName);
            FileOutputStream fileStream = new FileOutputStream(file);
            Writer writer = new OutputStreamWriter(fileStream, StandardCharsets.UTF_8);
            writer.write("/*\n" +
                " * Licensed to the Apache Software Foundation (ASF) under one or more\n" +
                " * contributor license agreements.  See the NOTICE file distributed with\n" +
                " * this work for additional information regarding copyright ownership.\n" +
                " * The ASF licenses this file to You under the Apache License, Version 2.0\n" +
                " * (the \"License\"); you may not use this file except in compliance with\n" +
                " * the License.  You may obtain a copy of the License at\n" +
                " *\n" +
                " *     http://www.apache.org/licenses/LICENSE-2.0\n" +
                " *\n" +
                " * Unless required by applicable law or agreed to in writing, software\n" +
                " * distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
                " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
                " * See the License for the specific language governing permissions and\n" +
                " * limitations under the License.\n" +
                " */\n\n\n");
            for (Map.Entry<String, String> kv: map.entrySet()) {
                writer.append(String.format("%s : %s\n", kv.getKey(), kv.getValue()));
            }
            writer.close();
        }
    }

    public static Map<String, TreeMap<String, String>> load(String base, String label) throws Exception {
        File dir = new File(String.format("%s/%s", base, label));
        Map<String, TreeMap<String, String>> schemaMap = new TreeMap<>();
        for (File file: dir.listFiles()) {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            String line = null;
            TreeMap<String, String> kvs = new TreeMap<>();
            while ((line = br.readLine()) != null) {
                if (line.contains("*")) {
                    continue;
                }
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] items = line.split(":");
                kvs.put(items[0].trim(), items[1].trim());
            }
            br.close();
            schemaMap.put(file.getName().replace(".schema", ""), kvs);
        }
        return schemaMap;
    }
}
