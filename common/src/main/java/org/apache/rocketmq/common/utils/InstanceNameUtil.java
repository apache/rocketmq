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

import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Random;
import org.apache.rocketmq.common.UtilAll;

public class InstanceNameUtil {

    public static String get() {
        int instanceId = getMachineId() | getProcessId();
        return String.valueOf(instanceId);
    }

    private static int getMachineId() {
        int machineId;
        try {
            StringBuilder sb = new StringBuilder();
            Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
            while (e.hasMoreElements()) {
                NetworkInterface ni = e.nextElement();
                if (!UtilAll.isBlank(ni.getName())) {
                    sb.append(ni.getName());
                }
                if (ni.getHardwareAddress() != null) {
                    sb.append(Arrays.toString(ni.getHardwareAddress()));
                }
                sb.append(ni.getMTU());
                Iterator<InterfaceAddress> it = ni.getInterfaceAddresses().iterator();
                while (it.hasNext()) {
                    InterfaceAddress ia = it.next();
                    if (ia.getAddress() != null) {
                        sb.append(ia.getAddress());
                    }
                }
            }
            machineId = sb.toString().hashCode() << 16;
        } catch (Throwable e) {
            // exception sometimes happens with IBM JVM, use random
            machineId = (new Random().nextInt()) << 16;
        }
        return Integer.toHexString(machineId).hashCode() & 0xFFFF;
    }

    private static int getProcessId() {
        int processPiece;
        int processId = new Random().nextInt();
        try {
            processId =
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
        } catch (Throwable t) {
        }
        ClassLoader loader = InstanceNameUtil.class.getClassLoader();
        int loaderId = loader != null ? System.identityHashCode(loader) : 0;
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(processId));
        sb.append(Integer.toHexString(loaderId));
        processPiece = sb.toString().hashCode() & 0xFFFF;
        return processPiece;
    }

}
