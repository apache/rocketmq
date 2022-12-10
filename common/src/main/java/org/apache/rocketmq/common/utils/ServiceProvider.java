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

import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ServiceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    /**
     * A reference to the classloader that loaded this class. It's more efficient to compute it once and cache it here.
     */
    private static ClassLoader thisClassLoader;
    
    /**
     * JDK1.3+ <a href= "http://java.sun.com/j2se/1.3/docs/guide/jar/jar.html#Service%20Provider" > 'Service Provider'
     * specification</a>.
     */
    public static final String PREFIX = "META-INF/service/";
    
    static {
        thisClassLoader = getClassLoader(ServiceProvider.class);
    }
    
    /**
     * Returns a string that uniquely identifies the specified object, including its class.
     * <p>
     * The returned string is of form "classname@hashcode", ie is the same as the return value of the Object.toString()
     * method, but works even when the specified object's class has overidden the toString method.
     *
     * @param o may be null.
     * @return a string of form classname@hashcode, or "null" if param o is null.
     */
    protected static String objectId(Object o) {
        if (o == null) {
            return "null";
        } else {
            return o.getClass().getName() + "@" + System.identityHashCode(o);
        }
    }
    
    protected static ClassLoader getClassLoader(Class<?> clazz) {
        try {
            return clazz.getClassLoader();
        } catch (SecurityException e) {
            LOG.error("Unable to get classloader for class {} due to security restrictions , error info {}",
                clazz, e.getMessage());
            throw e;
        }
    }
    
    protected static ClassLoader getContextClassLoader() {
        ClassLoader classLoader = null;
        try {
            classLoader = Thread.currentThread().getContextClassLoader();
        } catch (SecurityException ex) {
            /**
             * The getContextClassLoader() method throws SecurityException when the context
             * class loader isn't an ancestor of the calling class's class
             * loader, or if security permissions are restricted.
             */
        }
        return classLoader;
    }
    
    protected static InputStream getResourceAsStream(ClassLoader loader, String name) {
        if (loader != null) {
            return loader.getResourceAsStream(name);
        } else {
            return ClassLoader.getSystemResourceAsStream(name);
        }
    }
    
    public static <T> List<T> load(Class<?> clazz) {
        String fullName = PREFIX + clazz.getName();
        return load(fullName, clazz);
    }
    
    public static <T> List<T> load(String name, Class<?> clazz) {
        LOG.info("Looking for a resource file of name [{}] ...", name);
        List<T> services = new ArrayList<>();
        InputStream is = getResourceAsStream(getContextClassLoader(), name);
        if (is == null) {
            LOG.warn("No resource file with name [{}] found.", name);
            return services;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String serviceName = reader.readLine();
            List<String> names = new ArrayList<>();
            while (serviceName != null && !"".equals(serviceName)) {
                LOG.info(
                    "Creating an instance as specified by file {} which was present in the path of the context classloader.",
                    name);
                if (!names.contains(serviceName)) {
                    names.add(serviceName);
                    services.add(initService(getContextClassLoader(), serviceName, clazz));
                }
                serviceName = reader.readLine();
            }
        } catch (Exception e) {
            LOG.error("Error occurred when looking for resource file " + name, e);
        }
        return services;
    }
    
    public static <T> T loadClass(Class<?> clazz) {
        String fullName = PREFIX + clazz.getName();
        return loadClass(fullName, clazz);
    }
    
    public static <T> T loadClass(String name, Class<?> clazz) {
        LOG.info("Looking for a resource file of name [{}] ...", name);
        T s = null;
        InputStream is = getResourceAsStream(getContextClassLoader(), name);
        if (is == null) {
            LOG.warn("No resource file with name [{}] found.", name);
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String serviceName = reader.readLine();
            if (serviceName != null && !"".equals(serviceName)) {
                s = initService(getContextClassLoader(), serviceName, clazz);
            } else {
                LOG.warn("ServiceName is empty!");
            }
        } catch (Exception e) {
            LOG.warn("Error occurred when looking for resource file " + name, e);
        }
        return s;
    }

    protected static <T> T initService(ClassLoader classLoader, String serviceName, Class<?> clazz) {
        Class<?> serviceClazz = null;
        try {
            if (classLoader != null) {
                try {
                    // Warning: must typecast here & allow exception to be generated/caught & recast properly
                    serviceClazz = classLoader.loadClass(serviceName);
                    if (clazz.isAssignableFrom(serviceClazz)) {
                        LOG.info("Loaded class {} from classloader {}", serviceClazz.getName(),
                            objectId(classLoader));
                    } else {
                        // This indicates a problem with the ClassLoader tree. An incompatible ClassLoader was used to load the implementation.
                        LOG.error(
                            "Class {} loaded from classloader {} does not extend {} as loaded by this classloader.",
                            serviceClazz.getName(),
                            objectId(serviceClazz.getClassLoader()), clazz.getName());
                    }
                    return (T) serviceClazz.getDeclaredConstructor().newInstance();
                } catch (ClassNotFoundException ex) {
                    if (classLoader == thisClassLoader) {
                        // Nothing more to try, onwards.
                        LOG.warn("Unable to locate any class {} via classloader {}", serviceName,
                            objectId(classLoader));
                        throw ex;
                    }
                    // Ignore exception, continue
                } catch (NoClassDefFoundError e) {
                    if (classLoader == thisClassLoader) {
                        // Nothing more to try, onwards.
                        LOG.warn(
                            "Class {} cannot be loaded via classloader {}.it depends on some other class that cannot be found.",
                            serviceClazz, objectId(classLoader));
                        throw e;
                    }
                    // Ignore exception, continue
                }
            }
        } catch (Exception e) {
            LOG.error("Unable to init service.", e);
        }
        return (T) serviceClazz;
    }
}
