/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alibaba.rocketmq.common.annotation.ImportantField;
import com.alibaba.rocketmq.common.help.FAQUrl;


/**
 * 各种方法大杂烩
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
 */
public class MixAll {
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    public static final String MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    public static final String WS_DOMAIN_NAME = System.getProperty("rocketmq.namesrv.domain",
        "jmenv.tbsite.net");
    public static final String WS_DOMAIN_SUBGROUP = System.getProperty("rocketmq.namesrv.domain.subgroup",
        "nsaddr");
    // http://jmenv.tbsite.net:8080/rocketmq/nsaddr
    public static final String WS_ADDR = "http://" + WS_DOMAIN_NAME + ":8080/rocketmq/" + WS_DOMAIN_SUBGROUP;
    public static final String DEFAULT_TOPIC = "TBW102";
    public static final String BENCHMARK_TOPIC = "BenchmarkTest";
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    public static final String TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    public static final String FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    public static final String MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    public static final String CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
    public static final String SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
    public static final String SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
    public static final String SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    public static final String OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT";

    public static final List<String> LocalInetAddrs = getLocalInetAddress();
    public static final String Localhost = localhost();
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final long MASTER_ID = 0L;
    public static final long CURRENT_JVM_PID = getPID();
    // 为每个Consumer Group建立一个默认的Topic，前缀 + GroupName，用来保存处理失败需要重试的消息
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    // 为每个Consumer Group建立一个默认的Topic，前缀 + GroupName，用来保存重试多次都失败，接下来不再重试的消息
    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";


    public static String getRetryTopic(final String consumerGroup) {
        return RETRY_GROUP_TOPIC_PREFIX + consumerGroup;
    }


    public static String getDLQTopic(final String consumerGroup) {
        return DLQ_GROUP_TOPIC_PREFIX + consumerGroup;
    }


    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if (processName != null && processName.length() > 0) {
            try {
                return Long.parseLong(processName.split("@")[0]);
            }
            catch (Exception e) {
                return 0;
            }
        }

        return 0;
    }


    public static long createBrokerId(final String ip, final int port) {
        InetSocketAddress isa = new InetSocketAddress(ip, port);
        byte[] ipArray = isa.getAddress().getAddress();
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put(ipArray);
        bb.putInt(port);
        long value = bb.getLong(0);
        return Math.abs(value);
    }


    /**
     * 安全的写文件
     */
    public static final void string2File(final String str, final String fileName) throws IOException {
        // 先写入临时文件
        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(str, tmpFile);

        // 备份之前的文件
        String bakFile = fileName + ".bak";
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        // 删除正式文件
        File file = new File(fileName);
        file.delete();

        // 临时文件改为正式文件
        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }


    public static final void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(str);
        }
        catch (IOException e) {
            throw e;
        }
        finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                }
                catch (IOException e) {
                    throw e;
                }
            }
        }
    }


    public static final String file2String(final String fileName) {
        File file = new File(fileName);
        return file2String(file);
    }


    public static final String file2String(final URL url) {
        InputStream in = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            in = urlConnection.getInputStream();
            int len = in.available();
            byte[] data = new byte[len];
            in.read(data, 0, len);
            return new String(data, "UTF-8");
        }
        catch (Exception e) {
        }
        finally {
            if (null != in) {
                try {
                    in.close();
                }
                catch (IOException e) {
                }
            }
        }

        return null;
    }


    public static final String file2String(final File file) {
        if (file.exists()) {
            char[] data = new char[(int) file.length()];
            boolean result = false;

            FileReader fileReader = null;
            try {
                fileReader = new FileReader(file);
                int len = fileReader.read(data);
                result = (len == data.length);
            }
            catch (IOException e) {
                // e.printStackTrace();
            }
            finally {
                if (fileReader != null) {
                    try {
                        fileReader.close();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (result) {
                String value = new String(data);
                return value;
            }
        }
        return null;
    }


    public static String findClassPath(Class<?> c) {
        URL url = c.getProtectionDomain().getCodeSource().getLocation();
        return url.getPath();
    }


    public static void printObjectProperties(final Logger log, final Object object) {
        printObjectProperties(log, object, false);
    }


    public static void printObjectProperties(final Logger log, final Object object,
            final boolean onlyImportantField) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                        if (null == value) {
                            value = "";
                        }
                    }
                    catch (IllegalArgumentException e) {
                        System.out.println(e);
                    }
                    catch (IllegalAccessException e) {
                        System.out.println(e);
                    }

                    if (onlyImportantField) {
                        Annotation annotation = field.getAnnotation(ImportantField.class);
                        if (null == annotation) {
                            continue;
                        }
                    }

                    if (log != null) {
                        log.info(name + "=" + value);
                    }
                    else {
                        System.out.println(name + "=" + value);
                    }
                }
            }
        }
    }


    public static String properties2String(final Properties properties) {
        Set<Object> sets = properties.keySet();
        StringBuilder sb = new StringBuilder();
        for (Object key : sets) {
            Object value = properties.get(key);
            if (value != null) {
                sb.append(key.toString() + "=" + value.toString() + "\n");
            }
        }

        return sb.toString();
    }


    /**
     * 字符串转化成Properties 字符串和Properties配置文件格式一样
     */
    public static Properties string2Properties(final String str) {
        Properties properties = new Properties();
        try {
            InputStream in = new ByteArrayInputStream(str.getBytes(DEFAULT_CHARSET));
            properties.load(in);
        }
        catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return properties;
    }


    /**
     * 将对象各成员属性值转化为Properties
     */
    public static Properties object2Properties(final Object object) {
        Properties properties = new Properties();

        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                    }
                    catch (IllegalArgumentException e) {
                        System.out.println(e);
                    }
                    catch (IllegalAccessException e) {
                        System.out.println(e);
                    }

                    if (value != null) {
                        properties.setProperty(name, value.toString());
                    }
                }
            }
        }

        return properties;
    }


    /**
     * 将Properties中的值写入Object
     */
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
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg = null;
                            if (cn.equals("int")) {
                                arg = Integer.parseInt(property);
                            }
                            else if (cn.equals("long")) {
                                arg = Long.parseLong(property);
                            }
                            else if (cn.equals("double")) {
                                arg = Double.parseDouble(property);
                            }
                            else if (cn.equals("boolean")) {
                                arg = Boolean.parseBoolean(property);
                            }
                            else if (cn.equals("String")) {
                                arg = property;
                            }
                            else {
                                continue;
                            }
                            method.invoke(object, new Object[] { arg });
                        }
                    }
                }
                catch (Throwable e) {
                }
            }
        }
    }


    public static boolean isPropertiesEqual(final Properties p1, final Properties p2) {
        return p1.equals(p2);
    }


    public static List<String> getLocalInetAddress() {
        List<String> inetAddressList = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface networkInterface = enumeration.nextElement();
                Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    inetAddressList.add(addrs.nextElement().getHostAddress());
                }
            }
        }
        catch (SocketException e) {
            throw new RuntimeException("get local inet address fail", e);
        }

        return inetAddressList;
    }


    public static boolean isLocalAddr(String address) {
        for (String addr : LocalInetAddrs) {
            if (address.contains(addr))
                return true;
        }
        return false;
    }


    private static String localhost() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostAddress();
        }
        catch (Throwable e) {
            throw new RuntimeException(
                "InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException"
                        + FAQUrl.suggestTodo(FAQUrl.UNKNOWN_HOST_EXCEPTION), e);
        }
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


    public Set<String> list2Set(List<String> values) {
        Set<String> result = new HashSet<String>();
        for (String v : values) {
            result.add(v);
        }
        return result;
    }


    public List<String> set2List(Set<String> values) {
        List<String> result = new ArrayList<String>();
        for (String v : values) {
            result.add(v);
        }
        return result;
    }


    public static String localhostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (Throwable e) {
            throw new RuntimeException(
                "InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException"
                        + FAQUrl.suggestTodo(FAQUrl.UNKNOWN_HOST_EXCEPTION), e);
        }
    }
}
