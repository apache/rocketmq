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
package org.apache.rocketmq.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.IOTinyUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class MixAll {
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    public static final String MESSAGE_COMPRESS_TYPE = "rocketmq.message.compressType";
    public static final String MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    public static final String DEFAULT_NAMESRV_ADDR_LOOKUP = "jmenv.tbsite.net";
    public static final String WS_DOMAIN_NAME = System.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
    public static final String WS_DOMAIN_SUBGROUP = System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    public static final String TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    public static final String SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER";
    public static final String FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    public static final String MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    public static final String CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
    public static final String SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
    public static final String SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
    public static final String ONS_HTTP_PROXY_GROUP = "CID_ONS-HTTP-PROXY";
    public static final String CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
    public static final String CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
    public static final String CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
    public static final String CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";
    public static final String IS_SUPPORT_HEART_BEAT_V2 = "IS_SUPPORT_HEART_BEAT_V2";
    public static final String IS_SUB_CHANGE = "IS_SUB_CHANGE";
    public static final List<String> LOCAL_INET_ADDRESS = getLocalInetAddress();
    public static final String LOCALHOST = localhost();
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final long MASTER_ID = 0L;
    public static final long FIRST_SLAVE_ID = 1L;

    public static final long FIRST_BROKER_CONTROLLER_ID = 1L;
    public static final long CURRENT_JVM_PID = getPID();
    public final static int UNIT_PRE_SIZE_FOR_MSG = 28;
    public final static int ALL_ACK_IN_SYNC_STATE_SET = -1;

    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    public static final String REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
    public static final String UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY";
    public static final String DEFAULT_TRACE_REGION_ID = "DefaultRegion";
    public static final String CONSUME_CONTEXT_TYPE = "ConsumeContextType";
    public static final String CID_SYS_RMQ_TRANS = "CID_RMQ_SYS_TRANS";
    public static final String ACL_CONF_TOOLS_FILE = "/conf/tools.yml";
    public static final String REPLY_MESSAGE_FLAG = "reply";
    public static final String LMQ_PREFIX = "%LMQ%";
    public static final int LMQ_QUEUE_ID = 0;
    public static final String LMQ_DISPATCH_SEPARATOR = ",";
    public static final String REQ_T = "ReqT";
    public static final String ROCKETMQ_ZONE_ENV = "ROCKETMQ_ZONE";
    public static final String ROCKETMQ_ZONE_PROPERTY = "rocketmq.zone";
    public static final String ROCKETMQ_ZONE_MODE_ENV = "ROCKETMQ_ZONE_MODE";
    public static final String ROCKETMQ_ZONE_MODE_PROPERTY = "rocketmq.zone.mode";
    public static final String ZONE_NAME = "__ZONE_NAME";
    public static final String ZONE_MODE = "__ZONE_MODE";
    public final static String RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd";
    public final static String RPC_REQUEST_HEADER_NAMESPACE_FIELD = "ns";

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    public static final String LOGICAL_QUEUE_MOCK_BROKER_PREFIX = "__syslo__";
    public static final String METADATA_SCOPE_GLOBAL = "__global__";
    public static final String LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST = "__syslo__none__";
    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

    private static final String OS = System.getProperty("os.name").toLowerCase();

    public static boolean isWindows() {
        return OS.contains("win");
    }

    public static boolean isMac() {
        return OS.contains("mac");
    }

    public static boolean isJdk8() {
        return System.getProperty("java.version").startsWith("1.8.");
    }

    public static boolean isUnix() {
        return OS.contains("nix")
            || OS.contains("nux")
            || OS.contains("aix");
    }

    public static boolean isSolaris() {
        return OS.contains("sunos");
    }

    public static String getWSAddr() {
        String wsDomainName = System.getProperty("rocketmq.namesrv.domain", DEFAULT_NAMESRV_ADDR_LOOKUP);
        String wsDomainSubgroup = System.getProperty("rocketmq.namesrv.domain.subgroup", "nsaddr");
        String wsAddr = "http://" + wsDomainName + ":8080/rocketmq/" + wsDomainSubgroup;
        if (wsDomainName.indexOf(":") > 0) {
            wsAddr = "http://" + wsDomainName + "/rocketmq/" + wsDomainSubgroup;
        }
        return wsAddr;
    }

    public static String getRetryTopic(final String consumerGroup) {
        return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    public static String getReplyTopic(final String clusterName) {
        return clusterName + "_" + REPLY_TOPIC_POSTFIX;
    }

    public static boolean isSysConsumerGroup(final String consumerGroup) {
        return consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }

    public static String getDLQTopic(final String consumerGroup) {
        return DLQ_GROUP_TOPIC_PREFIX + consumerGroup;
    }

    public static String brokerVIPChannel(final boolean isChange, final String brokerAddr) {
        if (isChange) {
            int split = brokerAddr.lastIndexOf(":");
            String ip = brokerAddr.substring(0, split);
            String port = brokerAddr.substring(split + 1);
            String brokerAddrNew = ip + ":" + (Integer.parseInt(port) - 2);
            return brokerAddrNew;
        } else {
            return brokerAddr;
        }
    }

    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if (StringUtils.isNotEmpty(processName)) {
            try {
                return Long.parseLong(processName.split("@")[0]);
            } catch (Exception e) {
                return 0;
            }
        }

        return 0;
    }

    public static synchronized void string2File(final String str, final String fileName) throws IOException {

        String bakFile = fileName + ".bak";
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        string2FileNotSafe(str, fileName);
    }

    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        IOTinyUtils.writeStringToFile(file, str, DEFAULT_CHARSET);
    }

    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    public static String file2String(final File file) throws IOException {
        if (file.exists()) {
            byte[] data = new byte[(int) file.length()];
            boolean result;

            try (FileInputStream inputStream = new FileInputStream(file)) {
                int len = inputStream.read(data);
                result = len == data.length;
            }

            if (result) {
                return new String(data, DEFAULT_CHARSET);
            }
        }
        return null;
    }

    public static String file2String(final URL url) {
        InputStream in = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            in = urlConnection.getInputStream();
            int len = in.available();
            byte[] data = new byte[len];
            in.read(data, 0, len);
            return new String(data, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
        }

        return null;
    }

    public static void printObjectProperties(final Logger logger, final Object object) {
        printObjectProperties(logger, object, false);
    }

    public static void printObjectProperties(final Logger logger, final Object object,
        final boolean onlyImportantField) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    if (onlyImportantField) {
                        Annotation annotation = field.getAnnotation(ImportantField.class);
                        if (null == annotation) {
                            continue;
                        }
                    }

                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                        if (null == value) {
                            value = "";
                        }
                    } catch (IllegalAccessException e) {
                        log.error("Failed to obtain object properties", e);
                    }

                    if (logger != null) {
                        logger.info(name + "=" + value);
                    }
                }
            }
        }
    }

    public static String properties2String(final Properties properties) {
        return properties2String(properties, false);
    }

    public static String properties2String(final Properties properties, final boolean isSort) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<Object, Object>> entrySet = isSort ? new TreeMap<>(properties).entrySet() : properties.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            if (entry.getValue() != null) {
                sb.append(entry.getKey().toString() + "=" + entry.getValue().toString() + "\n");
            }
        }
        return sb.toString();
    }

    public static Properties string2Properties(final String str) {
        Properties properties = new Properties();
        try {
            InputStream in = new ByteArrayInputStream(str.getBytes(DEFAULT_CHARSET));
            properties.load(in);
        } catch (Exception e) {
            log.error("Failed to handle properties", e);
            return null;
        }

        return properties;
    }

    public static Properties object2Properties(final Object object) {
        Properties properties = new Properties();

        Class<?> objectClass = object.getClass();
        while (true) {
            Field[] fields = objectClass.getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(object);
                        } catch (IllegalAccessException e) {
                            log.error("Failed to handle properties", e);
                        }

                        if (value != null) {
                            properties.setProperty(name, value.toString());
                        }
                    }
                }
            }
            if (objectClass == Object.class || objectClass.getSuperclass() == Object.class) {
                break;
            }
            objectClass = objectClass.getSuperclass();
        }

        return properties;
    }

    public static void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                property = property.trim();
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public static boolean isPropertiesEqual(final Properties p1, final Properties p2) {
        return p1.equals(p2);
    }

    public static boolean isPropertyValid(Properties props, String key, Predicate<String> validator) {
        return validator.test(props.getProperty(key));
    }

    public static List<String> getLocalInetAddress() {
        List<String> inetAddressList = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface networkInterface = enumeration.nextElement();
                Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    inetAddressList.add(addrs.nextElement().getHostAddress());
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException("get local inet address fail", e);
        }

        return inetAddressList;
    }

    private static String localhost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Throwable e) {
            try {
                String candidatesHost = getLocalhostByNetworkInterface();
                if (candidatesHost != null)
                    return candidatesHost;

            } catch (Exception ignored) {
            }

            throw new RuntimeException("InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException" + FAQUrl.suggestTodo(FAQUrl.UNKNOWN_HOST_EXCEPTION), e);
        }
    }

    //Reverse logic comparing to RemotingUtil method, consider refactor in RocketMQ 5.0
    public static String getLocalhostByNetworkInterface() throws SocketException {
        List<String> candidatesHost = new ArrayList<>();
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();

        while (enumeration.hasMoreElements()) {
            NetworkInterface networkInterface = enumeration.nextElement();
            // Workaround for docker0 bridge
            if ("docker0".equals(networkInterface.getName()) || !networkInterface.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                InetAddress address = addrs.nextElement();
                if (address.isLoopbackAddress()) {
                    continue;
                }
                //ip4 higher priority
                if (address instanceof Inet6Address) {
                    candidatesHost.add(address.getHostAddress());
                    continue;
                }
                return address.getHostAddress();
            }
        }

        if (!candidatesHost.isEmpty()) {
            return candidatesHost.get(0);
        }

        // Fallback to loopback
        return localhost();
    }

    public static boolean compareAndIncreaseOnly(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                return true;

            prev = target.get();
        }

        return false;
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit)
            return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static int compareInteger(int x, int y) {
        return Integer.compare(x, y);
    }

    public static int compareLong(long x, long y) {
        return Long.compare(x, y);
    }

    public static boolean isLmq(String lmqMetaData) {
        return lmqMetaData != null && lmqMetaData.startsWith(LMQ_PREFIX);
    }

    public static String dealFilePath(String aclFilePath) {
        Path path = Paths.get(aclFilePath);
        return path.normalize().toString();
    }

    public static boolean isSysConsumerGroupForNoColdReadLimit(String consumerGroup) {
        if (DEFAULT_CONSUMER_GROUP.equals(consumerGroup)
            || TOOLS_CONSUMER_GROUP.equals(consumerGroup)
            || SCHEDULE_CONSUMER_GROUP.equals(consumerGroup)
            || FILTERSRV_CONSUMER_GROUP.equals(consumerGroup)
            || MONITOR_CONSUMER_GROUP.equals(consumerGroup)
            || SELF_TEST_CONSUMER_GROUP.equals(consumerGroup)
            || ONS_HTTP_PROXY_GROUP.equals(consumerGroup)
            || CID_ONSAPI_PERMISSION_GROUP.equals(consumerGroup)
            || CID_ONSAPI_OWNER_GROUP.equals(consumerGroup)
            || CID_ONSAPI_PULL_GROUP.equals(consumerGroup)
            || CID_SYS_RMQ_TRANS.equals(consumerGroup)
            || consumerGroup.startsWith(CID_RMQ_SYS_PREFIX)) {
            return true;
        }
        return false;
    }

    public static boolean topicAllowsLMQ(String topic) {
        return !topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !topic.startsWith(TopicValidator.SYSTEM_TOPIC_PREFIX)
            && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }
}
