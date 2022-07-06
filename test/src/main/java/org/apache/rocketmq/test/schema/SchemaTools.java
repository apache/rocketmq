package org.apache.rocketmq.test.schema;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
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
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
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


    public static void doLoad() {
        {
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
        {
            //basic
            apiClassList.add(DefaultMQPushConsumer.class);
            apiClassList.add(DefaultMQProducer.class);
            apiClassList.add(DefaultMQPullConsumer.class);
            apiClassList.add(DefaultLitePullConsumer.class);
            apiClassList.add(DefaultMQAdminExt.class);

            //argument
            apiClassList.add(Message.class);
            apiClassList.add(MessageQueue.class);
            apiClassList.add(SendCallback.class);
            apiClassList.add(MessageQueueSelector.class);
            //result
            apiClassList.add(MessageExt.class);
            apiClassList.add(ConsumeStatus.class);
            apiClassList.add(SendResult.class);
            apiClassList.add(SendStatus.class);
            //listener or context
            apiClassList.add(MessageListener.class);
            apiClassList.add(MessageListenerConcurrently.class);
            apiClassList.add(MessageListenerOrderly.class);
            apiClassList.add(ConsumeConcurrentlyContext.class);
            apiClassList.add(ConsumeOrderlyContext.class);
            apiClassList.add(ConsumeMessageContext.class);
            apiClassList.add(SendMessageContext.class);
            //hook
            apiClassList.add(RPCHook.class);
            apiClassList.add(SendMessageHook.class);
            apiClassList.add(ConsumeMessageHook.class);
            apiClassList.add(EndTransactionHook.class);

        }
        {
            protocolClassList.add(RequestCode.class);
            Reflections reflections = new Reflections("org.apache.rocketmq");
            for (Class<?> protocolClass: reflections.getSubTypesOf(CommandCustomHeader.class)) {
                protocolClassList.add(protocolClass);
            }
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
            && !Modifier.isAbstract(apiClass.getModifiers())
            && !apiClass.isEnum()) {
            Constructor<?> constructor = null;
            for (Constructor<?> tmp: apiClass.getConstructors()) {
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
            obj = constructor.newInstance(Arrays.stream(constructor.getParameterTypes()).map(x -> {
                try {
                    return x.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(msg, e);
                }
            }).toArray());
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
            Object fieldValue = "null";
            try {
                 fieldValue = field.get(obj);
            } catch (Exception e) {
                //System.out.println(apiClass.getName() + " " + field.getName());
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


    public static void doGen(List<Class<?>> classList, String label) throws Exception {
        for (Class<?> apiClass : classList) {
            TreeMap<String, String> map = buildSchemaOfFields(apiClass);
            map.putAll(buildSchemaOfMethods(apiClass));
            File file = new File(String.format("%s/%s/%s.schema", BASE_SCHEMA_PATH, label, apiClass.getName()));
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write("");
            for (Map.Entry<String, String> entry: map.entrySet()) {
                fileWriter.append(String.format("%s : %s\n", entry.getKey(), entry.getValue()));
            }
            fileWriter.close();
        }
    }

    public static void doGen() throws Exception {
        doLoad();
        doGen(apiClassList, "api");
        doGen(protocolClassList, "protocol");
    }
}
