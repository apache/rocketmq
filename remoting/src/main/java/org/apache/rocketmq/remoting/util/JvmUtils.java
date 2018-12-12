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

package org.apache.rocketmq.remoting.util;

import io.netty.channel.epoll.Epoll;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Random;

public final class JvmUtils {
    public static final String OS_NAME = System.getProperty("os.name").toLowerCase();
    //public static final String OS_VERSION = System.getProperty("os.version").toLowerCase();

    /**
     * A constructor to stop this class being constructed.
     */
    private JvmUtils() {
        // Unused
    }

    public static boolean isWindows() {
        return OS_NAME.startsWith("win");
    }

    public static boolean isWindows10() {
        return OS_NAME.startsWith("win") && OS_NAME.endsWith("10");
    }

    public static boolean isMacOSX() {
        return OS_NAME.contains("mac");
    }

    public static boolean isLinux() {
        return OS_NAME.startsWith("linux");
    }

    public static boolean isUseEpoll() {
        return isLinux() && Epoll.isAvailable();
    }

    public static boolean isUnix() {
        return OS_NAME.contains("nix") ||
            OS_NAME.contains("nux") ||
            OS_NAME.contains("aix") ||
            OS_NAME.contains("bsd") ||
            OS_NAME.contains("sun") ||
            OS_NAME.contains("hpux");
    }

    public static boolean isSolaris() {
        return OS_NAME.startsWith("sun");
    }

    public static int getProcessId() {
        String pid = null;
        final File self = new File("/proc/self");
        try {
            if (self.exists()) {
                pid = self.getCanonicalFile().getName();
            }
        } catch (IOException ignored) {
            //Ignore it
        }

        if (pid == null) {
            pid = ManagementFactory.getRuntimeMXBean().getName().split("@", 0)[0];
        }

        if (pid == null) {
            int rpid = new Random().nextInt(1 << 16);
            return rpid;
        } else {
            return Integer.parseInt(pid);
        }
    }

}

