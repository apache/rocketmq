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

package org.apache.rocketmq.rpc.internal;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.TreeSet;
import org.apache.rocketmq.remoting.api.serializable.Serializer;
import org.apache.rocketmq.remoting.impl.protocol.serializer.SerializerFactoryImpl;
import org.apache.rocketmq.rpc.annotation.RemoteMethod;
import org.apache.rocketmq.rpc.annotation.RemoteService;
import org.apache.rocketmq.rpc.impl.service.RpcEntry;

public class ServiceUtil {
    private static int methodExceptionLimit = 7;
    private static int methodEmbeddedGenericParameterLimit = 2;
    private static int methodSuperInterfaceLimit = 128;
    private static int methodParameterLimit = 7;
    private static SerializerFactoryImpl serializerFactory = new SerializerFactoryImpl();

    public static String toRequestCode(final RemoteService sc, Method method) {
        RemoteMethod asynchronous = method.getAnnotation(RemoteMethod.class);
        if (asynchronous != null) {
            return String.format("%s_%s/%s_%s", sc.name(), sc.version(),
                asynchronous.name().isEmpty() ? sharedToString(method) : asynchronous.name(),
                asynchronous.version());
        } else
            throw new IllegalArgumentException("RemoteMethod annotation not exist in method " + method.getName());
    }

    public static Set<String> getRequestCode(final Object obj) {
        Set<String> requestCodeSet = new TreeSet<>();
        Class<?>[] interfaces = obj.getClass().getInterfaces();
        for (Class<?> itf : interfaces) {
            RemoteService serviceExport = itf.getAnnotation(RemoteService.class);
            if (null == serviceExport) {
                throw new IllegalArgumentException("The Interface:" + itf.getName() + " do not remark RemoteService annotationType");
            }

            Method[] methods = itf.getMethods();
            for (Method method : methods) {
                if (method.getAnnotation(RemoteMethod.class) != null) {
                    if (ServiceUtil.testServiceExportMethod(method)) {
                        String requestCode = ServiceUtil.toRequestCode(serviceExport, method);
                        RpcEntry se = new RpcEntry();
                        se.setServiceExport(serviceExport);
                        se.setObject(obj);
                        se.setMethod(method);
                        requestCodeSet.add(requestCode);
                    }
                } else {
                    throw new IllegalArgumentException("RemoteMethod annotation not exist in method " + method.getName());
                }
            }
        }

        if (requestCodeSet.isEmpty()) {
            throw new IllegalArgumentException("interface no method or interface not set annotation");
        }
        return requestCodeSet;
    }

    private static boolean isGenericParameterDepthMuchLimit(Type type, int count) {
        if (count > methodEmbeddedGenericParameterLimit)
            return true;
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] types = parameterizedType.getActualTypeArguments();
            boolean isGenericParameter;
            for (final Type typ : types) {
                isGenericParameter = isGenericParameterDepthMuchLimit(typ, count + 1);
                if (isGenericParameter)
                    return isGenericParameter;
            }
        }
        return false;
    }

    private static boolean getGenericParameterTypes(Type[] types) {
        boolean genericFlag = false;
        for (Type type : types) {
            genericFlag |= isGenericParameterDepthMuchLimit(type, 0);
        }
        return genericFlag;
    }

    private static boolean isInterfaceInHeritDepthMuchLimit(Class<?> obj, int count) {
        if (count > methodSuperInterfaceLimit)
            return true;
        final RemoteService serviceExport = obj.getAnnotation(RemoteService.class);
        if (serviceExport == null)
            return false;
        Class<?>[] types = obj.getInterfaces();
        boolean isGenericParameter = false;
        for (Class<?> type : types) {
            isGenericParameter |= isInterfaceInHeritDepthMuchLimit(type, count + 1);
        }
        return isGenericParameter;
    }

    private static boolean isSeriliaze(Class<?> type) {
        return Serializable.class.isAssignableFrom(type);
    }

    public static boolean testServiceExportMethod(Method method) {
        Class<?>[] exceptions = method.getExceptionTypes();
        Type[] types = method.getGenericParameterTypes();
        if (method.getParameterTypes() != null
            && method.getParameterTypes().length > methodParameterLimit)
            throw new IllegalArgumentException("method: " + method.getName() + " argument more than max limit : "
                + methodParameterLimit);
        if (exceptions != null && exceptions.length > methodExceptionLimit)
            throw new IllegalArgumentException(" method: " + method.getName() + " exception more than max limit : "
                + methodExceptionLimit);

        if (getGenericParameterTypes(types))
            throw new IllegalArgumentException(" method: " + method.getName() + " generic argument depth more than max limit : "
                + methodEmbeddedGenericParameterLimit);

        Class<?>[] classObjs = method.getClass().getInterfaces();
        boolean isInterfaceHeritDepthMuch = false;
        for (Class classObj : classObjs) {
            isInterfaceHeritDepthMuch |= isInterfaceInHeritDepthMuchLimit(classObj, 0);
        }
        if (isInterfaceHeritDepthMuch) {
            throw new IllegalArgumentException(" method: " + method.getName() + " interface inherit depth more than max limit : "
                + methodSuperInterfaceLimit);
        }
        /*
        Class<?>[] tys = method.getParameterTypes();
        for (Class type : tys) {
            if (!isSeriliaze(type)) {
                throw new IllegalArgumentException("method parameter type:" + type.getName() + " can not Seriliaze !");
            }
        }
        */
        return true;
    }

    private static String sharedToString(Method method) {
        try {
            StringBuilder sb = new StringBuilder();

            String methodName = method.getName();
            if (methodName.contains("Async")) {
                int index = methodName.indexOf("Async");
                methodName = methodName.substring(0, index);
            }
            if (methodName.contains("Oneway")) {
                int index = methodName.indexOf("Oneway");
                methodName = methodName.substring(0, index);
            }
            sb.append(methodName);
            sb.append('<');

            Class<?>[] types = method.getParameterTypes();
            for (int j = 0; j < types.length; j++) {
                sb.append(types[j].getCanonicalName());
                if (j < (types.length - 1))
                    sb.append(",");
            }
            sb.append('>');
            return sb.toString();
        } catch (Exception e) {
            return "<" + e + ">";
        }
    }

    private static boolean isSerializer(Object[] args, Serializer serializer) {
        boolean success;
        if (serializer == null)
            return false;
        try {
            for (Object arg : args) {
                serializer.encode(arg);
            }
            success = true;
        } catch (Exception e) {
            success = false;
        }
        return success;
    }

    private static Serializer selectSerialization(Object[] args, Serializer defaultSerializer) {
        if (!isSerializer(args, defaultSerializer)) {
            Serializer[] serializers = serializerFactory.getTables();
            for (Serializer serializer : serializers) {
                if (isSerializer(args, serializer))
                    return serializer;
            }
        } else
            return defaultSerializer;
        return null;
    }

    public static Serializer selectSerializer(Object[] args, byte type) {
        Serializer defaultSerializer = serializerFactory.get(type);
        return ServiceUtil.selectSerialization(args, defaultSerializer);
    }
}
