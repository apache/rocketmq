package org.apache.rocketmq.test.schema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;

public class SchemaTools {
    public static String isPublicOrPrivate(int modifiers) {
        if (Modifier.isPublic(modifiers)) {
            return "public";
        } else if (Modifier.isProtected(modifiers)){
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
            && !Modifier.isAbstract(apiClass.getModifiers())) {
            obj = apiClass.newInstance();
        }
        TreeMap<String, String> map = new TreeMap<>();
        for (Field field: fields) {
            field.setAccessible(true);
            String key = String.format("Field %s", field.getName());
            String value = String.format("%s %s %s", isPublicOrPrivate(field.getModifiers()), field.getType().getName(), field.get(obj));
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
        for (Method method: methods) {
            String key = String.format("Method %s", method.getName());
            Class<?>[] parameterTypes = method.getParameterTypes();
            Arrays.sort(parameterTypes, new Comparator<Class<?>>() {
                @Override public int compare(Class<?> o1, Class<?> o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            String value = String.format("%s %s (%s)",
                isPublicOrPrivate(method.getModifiers()),
                method.getReturnType().getName(),
                Arrays.stream(parameterTypes).map(Class::getName).collect(Collectors.joining(",")));
            map.put(key, value);
        }
        return map;
    }

    public static void print(Class apiClass) throws Exception {
        TreeMap<String, String> map = buildSchemaOfFields(apiClass);
        map.putAll(buildSchemaOfMethods(apiClass));
        for (String key : map.keySet()) {
            System.out.printf("%s : %s\n", key, map.get(key));

        }
    }

    public static void main(String[] args) throws Exception {
        //print(SendMessageRequestHeader.class);
    }
}
