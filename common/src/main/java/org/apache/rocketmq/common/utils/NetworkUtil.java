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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class NetworkUtil {
    public static final String OS_NAME = System.getProperty("os.name");

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    private static boolean isLinuxPlatform = false;
    private static boolean isWindowsPlatform = false;

    static {
        if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
            isLinuxPlatform = true;
        }

        if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
            isWindowsPlatform = true;
        }
    }

    public static boolean isWindowsPlatform() {
        return isWindowsPlatform;
    }

    public static Selector openSelector() throws IOException {
        Selector result = null;

        if (isLinuxPlatform()) {
            try {
                final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                try {
                    final Method method = providerClazz.getMethod("provider");
                    final SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
                    if (selectorProvider != null) {
                        result = selectorProvider.openSelector();
                    }
                } catch (final Exception e) {
                    log.warn("Open ePoll Selector for linux platform exception", e);
                }
            } catch (final Exception e) {
                // ignore
            }
        }

        if (result == null) {
            result = Selector.open();
        }

        return result;
    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

    public static List<InetAddress> getLocalInetAddressList() throws SocketException {
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
        List<InetAddress> inetAddressList = new ArrayList<>();
        // Traversal Network interface to get the non-bridge and non-virtual and non-ppp and up address
        while (enumeration.hasMoreElements()) {
            final NetworkInterface nif = enumeration.nextElement();
            if (isBridge(nif) || nif.isVirtual() || nif.isPointToPoint() || !nif.isUp()) {
                continue;
            }
            InetAddressValidator validator = InetAddressValidator.getInstance();
            final Enumeration<InetAddress> en = nif.getInetAddresses();
            while (en.hasMoreElements()) {
                final InetAddress address = en.nextElement();
                if (address instanceof Inet4Address) {
                    byte[] ipByte = address.getAddress();
                    if (ipByte.length == 4) {
                        if (validator.isValidInet4Address(UtilAll.ipToIPv4Str(ipByte))) {
                            inetAddressList.add(address);
                        }
                    }
                } else if (address instanceof Inet6Address) {
                    byte[] ipByte = address.getAddress();
                    if (ipByte.length == 16) {
                        if (validator.isValidInet6Address(UtilAll.ipToIPv6Str(ipByte))) {
                            inetAddressList.add(address);
                        }
                    }
                }
            }
        }
        return inetAddressList;
    }

    public static InetAddress getLocalInetAddress() {
        try {
            ArrayList<InetAddress> ipv4Result = new ArrayList<>();
            ArrayList<InetAddress> ipv6Result = new ArrayList<>();
            List<InetAddress> localInetAddressList = getLocalInetAddressList();
            for (InetAddress inetAddress : localInetAddressList) {
                if (inetAddress instanceof Inet6Address) {
                    ipv6Result.add(inetAddress);
                } else {
                    ipv4Result.add(inetAddress);
                }
            }
            // prefer ipv4 and prefer external ip
            if (!ipv4Result.isEmpty()) {
                for (InetAddress ip : ipv4Result) {
                    if (UtilAll.isInternalIP(ip.getAddress())) {
                        continue;
                    }
                    return ip;
                }
                return ipv4Result.get(ipv4Result.size() - 1);
            } else if (!ipv6Result.isEmpty()) {
                for (InetAddress ip : ipv6Result) {
                    if (UtilAll.isInternalV6IP(ip)) {
                        continue;
                    }
                    return ip;
                }
                return ipv6Result.get(0);
            }
            //If failed to find,fall back to localhost
            return InetAddress.getLocalHost();
        } catch (Exception e) {
            log.error("Failed to obtain local address", e);
        }

        return null;
    }

    public static String getLocalAddress() {
        InetAddress localHost = getLocalInetAddress();
        return normalizeHostAddress(localHost);
    }

    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }

    public static String socketAddress2String(final SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getHostAddress());
        sb.append(":");
        sb.append(inetSocketAddress.getPort());
        return sb.toString();
    }

    public static String convert2IpString(final String addr) {
        return socketAddress2String(string2SocketAddress(addr));
    }

    private static boolean isBridge(NetworkInterface networkInterface) {
        try {
            if (isLinuxPlatform()) {
                String interfaceName = networkInterface.getName();
                File file = new File("/sys/class/net/" + interfaceName + "/bridge");
                return file.exists();
            }
        } catch (SecurityException e) {
            //Ignore
        }
        return false;
    }
}
