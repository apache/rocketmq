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

package org.apache.rocketmq.remoting.internal;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import org.jetbrains.annotations.NotNull;

public final class NetworkUtils {

    public static final String DEFAULT_LOCAL_ADDRESS = "127.0.0.1";
    public static final String DEFAULT_LOCAL_HOSTNAME = "localhost";

    /**
     * A constructor to stop this class being constructed.
     */
    private NetworkUtils() {
        // Unused
    }

    public static InetAddress getLoopbackAddress() {
        try {
            return InetAddress.getByName(null);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isLocalhost(@NotNull String host) {
        return host.equalsIgnoreCase(DEFAULT_LOCAL_HOSTNAME) || host.equals(DEFAULT_LOCAL_ADDRESS);
    }

    public static String getLocalHostIp() {
        try {
            for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = ifaces.nextElement();
                // Workaround for docker0 bridge
                if ("docker0".equals(iface.getName()) || !iface.isUp()) {
                    continue;
                }
                InetAddress ia;
                for (Enumeration<InetAddress> ips = iface.getInetAddresses(); ips.hasMoreElements(); ) {
                    ia = ips.nextElement();
                    if (ia instanceof Inet4Address) {
                        // Check if the address is any local or loop back(127.0.0.1 or ::1)
                        if (!ia.isLoopbackAddress() && ia.getHostAddress().indexOf(':') == -1) {
                            if (ia.isSiteLocalAddress()) {
                                return ia.getHostAddress();
                            } else if (!ia.isLinkLocalAddress() && !ia.isAnyLocalAddress()
                                && !ia.isMulticastAddress()) {
                                return ia.getHostAddress();
                            }
                        }
                    }
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException("Could not get local host ip", e);
        }
        return DEFAULT_LOCAL_ADDRESS;
    }
}
