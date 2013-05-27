package com.alibaba.rocketmq.common;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;


/**
 * 各种方法大杂烩
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author lansheng.zj@taobao.com
 * 
 */
public class MixAll {
    public static final int PERM_PRIORITY = 0x1 << 3;
    public static final int PERM_READ = 0x1 << 2;
    public static final int PERM_WRITE = 0x1 << 1;
    public static final int PERM_INHERIT = 0x1 << 0;

    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";

    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";

    public static final String WS_DOMAIN_NAME = "diamondserver.tbsite.net";
    // http://diamondserver.tbsite.net:8080/rocketmq/nsaddr
    public static final String WS_ADDR = "http://" + WS_DOMAIN_NAME + ":8080/rocketmq/nsaddr";
    public static final String DEFAULT_TOPIC = "TBW102";
    public static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    public static final String DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    public static final String SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    public static final String NamesrvLoggerName = "RocketmqNamesrv";
    public static final String BrokerLoggerName = "RocketmqBroker";
    public static final String ClientLoggerName = "RocketmqClient";
    public static final String ToolsLoggerName = "RocketmqTools";
    public static final String CommonLoggerName = "RocketmqCommon";
    public static final String StoreLoggerName = "RocketmqStore";
    public static final String TransactionLoggerName = "RocketmqTransaction";
    public static final long TotalPhysicalMemorySize = getTotalPhysicalMemorySize();
    public static final List<String> LocalInetAddrs = getLocalInetAddress();
    public static final String Localhost = localhost();
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final long MASTER_ID = 0L;
    public static final long CURRENT_JVM_PID = getPID();


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


    public static final String file2String(final String fileName) {
        File file = new File(fileName);
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
            if (fileReader != null)
                try {
                    fileReader.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
        }

        if (result) {
            String value = new String(data);
            return value;
        }

        return null;
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


    public static final boolean string2File(final String str, final String fileName) {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(str);
            return true;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if (fileWriter != null)
                try {
                    fileWriter.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
        }

        return false;
    }


    public static boolean isReadable(final int perm) {
        return (perm & PERM_READ) == PERM_READ;
    }


    public static boolean isWriteable(final int perm) {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }


    public static boolean isInherited(final int perm) {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }


    public static String perm2String(final int perm) {
        final StringBuffer sb = new StringBuffer("---");
        if (isReadable(perm)) {
            sb.replace(0, 1, "R");
        }

        if (isWriteable(perm)) {
            sb.replace(1, 2, "W");
        }

        if (isInherited(perm)) {
            sb.replace(2, 3, "X");
        }

        return sb.toString();
    }


    public static String findClassPath(Class<?> c) {
        URL url = c.getProtectionDomain().getCodeSource().getLocation();
        return url.getPath();
    }


    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
                new Option("n", "namesrvAddr", true,
                    "Name server address list, eg: 192.168.1.100:9876;192.168.1.101:9876");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
            CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                return null;
            }
        }
        catch (ParseException e) {
            hf.printHelp(appName, options, true);
        }

        return commandLine;
    }


    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }


    public static void printObjectProperties(final Logger log, final Object object) {
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


    /**
     * 获取机器的物理内存
     * 
     * @return 单位字节
     */
    public static long getTotalPhysicalMemorySize() {
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long physicalTotal = osmxb.getTotalPhysicalMemorySize();
        return physicalTotal;
    }


    public static String properties2String(final Properties properties) {
        Set<Object> sets = properties.keySet();
        StringBuilder sb = new StringBuilder();
        for (Object key : sets) {
            Object value = properties.get(key);
            if (value != null) {
                sb.append(key.toString() + "=" + value.toString() + IOUtils.LINE_SEPARATOR);
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


    public static Logger createLogger(final String file, final String level) {
        // TODO
        return LoggerFactory.getLogger(ClientLoggerName);
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
        catch (UnknownHostException e) {
            throw new RuntimeException("get localhost fail", e);
        }
    }

}
