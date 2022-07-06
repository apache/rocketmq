package org.apache.rocketmq.test.schema;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.reflections.Reflections;

public class SchemaTools {
    public static final String BASE_SCHEMA_PATH = "test/src/main/resources/schema";
    public static final Map<Class<?>, Set<String>> ignoredFields = new HashMap<>();
    //Use name as the key instead of X.class directly. X.class is not equal to field.getType().
    public static final Set<String> fieldClassNames = new HashSet<>();
    public static final List<Class<?>> apiClassList = new ArrayList<>();
    public static final List<Class<?>> protocolClassList = new ArrayList<>();


    static {
        ignoredFields.put(ClientConfig.class, Sets.newHashSet("namesrvAddr", "clientIP", "clientCallbackExecutorThreads"));
        ignoredFields.put(DefaultLitePullConsumer.class, Sets.newHashSet("consumeTimestamp"));
        ignoredFields.put(DefaultMQPushConsumer.class, Sets.newHashSet("consumeTimestamp"));
        fieldClassNames.add(String.class.getName());
        fieldClassNames.add(Long.class.getName());
        fieldClassNames.add(Integer.class.getName());
        fieldClassNames.add(Short.class.getName());
        fieldClassNames.add(Byte.class.getName());
        fieldClassNames.add(Double.class.getName());
        fieldClassNames.add(Float.class.getName());
        fieldClassNames.add(Boolean.class.getName());
    }

    static {
        apiClassList.add(DefaultMQPushConsumer.class);
        apiClassList.add(DefaultMQProducer.class);
        apiClassList.add(DefaultMQPullConsumer.class);
        apiClassList.add(DefaultLitePullConsumer.class);
        apiClassList.add(DefaultMQAdminExt.class);
        apiClassList.add(RPCHook.class);
        apiClassList.add(SendCallback.class);
        apiClassList.add(MessageListener.class);
        apiClassList.add(MessageListenerConcurrently.class);
        apiClassList.add(MessageListenerOrderly.class);
        apiClassList.add(SendMessageHook.class);
        apiClassList.add(ConsumeMessageHook.class);
        apiClassList.add(EndTransactionHook.class);
    }

    static {
        protocolClassList.add(RequestCode.class);
        Reflections reflections = new Reflections("org.apache.rocketmq");
        for (Class<?> protocolClass: reflections.getSubTypesOf(CommandCustomHeader.class)) {
            System.out.println(protocolClass.getName());
        }

    }

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
            String key = String.format("Field %s", field.getName());
            boolean ignore = false;
            for (Class<?> tmpClass: ignoredFields.keySet()) {
                if (tmpClass.isAssignableFrom(apiClass)
                    && ignoredFields.get(tmpClass).contains(field.getName())) {
                    ignore = true;
                    //System.out.printf("Ignore AAA:%s %s %s\n", apiClass.getName(), field.getName(), field.getType().getName());
                    break;
                }
            }
            if (!field.getType().isEnum()
                && !field.getType().isPrimitive()
                && !fieldClassNames.contains(field.getType().getName())) {
                //System.out.printf("Ignore BBB:%s %s %s\n", apiClass.getName(), field.getName(), field.getType().getName());
                ignore = true;
            }
            field.setAccessible(true);
            Object fieldValue = field.get(obj);
            if (ignore) {
                System.out.printf("Ignore:%s %s %s\n", apiClass.getName(), field.getName(), field.getType().getName());
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
        for (Method method: methods) {
            if (!Modifier.isPublic(method.getModifiers())) {
                //only care for the public methods
                continue;
            }
            Class<?>[] parameterTypes = method.getParameterTypes();
            Arrays.sort(parameterTypes, new Comparator<Class<?>>() {
                @Override public int compare(Class<?> o1, Class<?> o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            Class<?>[] exceptionTypes = method.getExceptionTypes();
            Arrays.sort(exceptionTypes, new Comparator<Class<?>>() {
                @Override public int compare(Class<?> o1, Class<?> o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
            String key = String.format("Method %s(%s)", method.getName(), Arrays.stream(parameterTypes).map(Class::getName).collect(Collectors.joining(",")));
            String value = String.format("%s throws (%s)",
                isPublicOrPrivate(method.getModifiers()),
                method.getReturnType().getName(),
                Arrays.stream(exceptionTypes).map(Class::getName).collect(Collectors.joining(",")));
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

    public static void doGen() throws Exception {

    }

    public static void main(String[] args) throws Exception {

        for (Class<?> apiClass : apiClassList) {
            TreeMap<String, String> map = buildSchemaOfFields(apiClass);
            map.putAll(buildSchemaOfMethods(apiClass));
            File file = new File(String.format("%s/%s.schema", BASE_SCHEMA_PATH, apiClass.getName()));
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write("");
            for (Map.Entry<String, String> entry: map.entrySet()) {
                fileWriter.append(String.format("%s : %s\n", entry.getKey(), entry.getValue()));
            }
            fileWriter.close();
        }
    }
}
