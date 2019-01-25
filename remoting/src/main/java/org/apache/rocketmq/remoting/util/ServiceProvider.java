/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.rocketmq.remoting.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ServiceProvider {

    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * A reference to the classloader that loaded this class. It's more efficient to compute it once and cache it here.
     */
    private static ClassLoader thisClassLoader;

    /**
     * JDK1.3+ <a href= "http://java.sun.com/j2se/1.3/docs/guide/jar/jar.html#Service%20Provider" > 'Service Provider'
     * specification</a>.
     */
    public static final String TRANSACTION_SERVICE_ID = "META-INF/service/org.apache.rocketmq.broker.transaction.TransactionalMessageService";

    public static final String TRANSACTION_LISTENER_ID = "META-INF/service/org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener";

    public static final String ACL_VALIDATOR_ID = "META-INF/service/org.apache.rocketmq.acl.AccessValidator";

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
            LOG.error("Unable to get classloader for class {} due to security restrictions !",
                clazz, e.getMessage());
            throw e;
        }
    }

    public static ClassLoader getContextClassLoader() {
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

    public static InputStream getResourceAsStream(ClassLoader loader, String name) {
        if (loader != null) {
            return loader.getResourceAsStream(name);
        } else {
            return ClassLoader.getSystemResourceAsStream(name);
        }
    }

    public static <T> List<T> loadServiceList(String name, Class<?> clazz) {
        LOG.info("Looking for a resource file of name [{}] ...", name);
        List<T> services = new ArrayList<T>();
        try {
            ArrayList<String> names = new ArrayList<String>();
            final InputStream is = getResourceAsStream(getContextClassLoader(), name);
            if (is != null) {
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                } catch (java.io.UnsupportedEncodingException e) {
                    reader = new BufferedReader(new InputStreamReader(is));
                }
                String serviceName = reader.readLine();
                while (serviceName != null && !"".equals(serviceName)) {
                    if (!names.contains(serviceName)) {
                        T instance = createInstance(serviceName, clazz);
                        services.add(instance);
                    }
                    names.add(serviceName);
                    serviceName = reader.readLine();
                }
                reader.close();
            } else {
                // is == null
                LOG.warn("No resource file with name [{}] found.", name);
            }
        } catch (Exception e) {
            LOG.error("Error occured when looking for resource file " + name, e);
        }
        return services;
    }

    public static Map<String, String> loadPath(String path) {
        LOG.info("Load path looking for a resource file of name [{}] ...", path);
        Map<String, String> pathMap = new HashMap<String, String>();
        try {
            final InputStream is = getResourceAsStream(getContextClassLoader(), path);
            if (is != null) {
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                } catch (java.io.UnsupportedEncodingException e) {
                    reader = new BufferedReader(new InputStreamReader(is));
                }
                String serviceName = reader.readLine();
                while (serviceName != null && !"".equals(serviceName)) {
                    String[] service = serviceName.split("=");
                    if (service.length == 2) {
                        if (pathMap.containsKey(service[0])) {
                            continue;
                        } else {
                            pathMap.put(service[0], service[1]);
                        }
                    } else {
                        continue;
                    }
                    serviceName = reader.readLine();
                }
                reader.close();
            }
        } catch (Exception ex) {
            LOG.error("Error occurred when looking for resource file " + path, ex);
        }
        return pathMap;
    }

    public static <T> Map<String, T> load(String path, Class<?> clazz) {
        LOG.info("Load map is looking for a resource file of name [{}] ...", path);
        Map<String, T> services = new HashMap<String, T>();
        Map<String, String> pathMaps = loadPath(path);
        for (Map.Entry<String, String> entry : pathMaps.entrySet()) {
            T instance = (T) createInstance(entry.getValue(), clazz);
            if (instance != null && !services.containsKey(entry.getKey())) {
                services.put(entry.getKey(), instance);
            }
        }
        return services;
    }

    public static <T> T loadClass(String name, Class<?> clazz) {
        final InputStream is = getResourceAsStream(getContextClassLoader(), name);
        if (is != null) {
            BufferedReader reader;
            try {
                try {
                    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                } catch (java.io.UnsupportedEncodingException e) {
                    reader = new BufferedReader(new InputStreamReader(is));
                }
                String serviceName = reader.readLine();
                reader.close();
                return createInstance(serviceName, clazz);
            } catch (Exception e) {
                LOG.warn("Error occurred when looking for resource file " + name, e);
            }
        }
        return null;
    }

    public static <T> T createInstance(String serviceName, Class<?> clazz) {
        if (serviceName != null && !"".equals(serviceName)) {
            return initService(getContextClassLoader(), serviceName, clazz);
        } else {
            LOG.warn("ServiceName is empty!");
            return null;
        }
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
                            new Object[] {
                                serviceClazz.getName(),
                                objectId(serviceClazz.getClassLoader()), clazz.getName()});
                    }
                    return (T) serviceClazz.newInstance();
                } catch (ClassNotFoundException ex) {
                    if (classLoader == thisClassLoader) {
                        // Nothing more to try, onwards.
                        LOG.warn("Unable to locate any class {} via classloader", serviceName,
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
