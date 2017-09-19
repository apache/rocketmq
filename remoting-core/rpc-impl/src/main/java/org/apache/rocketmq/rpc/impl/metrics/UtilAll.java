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

package org.apache.rocketmq.rpc.impl.metrics;

import java.util.Map;

public class UtilAll {

    public static String jstack() {
        return jstack(Thread.getAllStackTraces());
    }

    private static String jstack(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            for (final Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    String threadName = entry.getKey().getName();
                    result.append(String.format("%-40s TID: %d STATE: %s\n", threadName, thread.getId(), thread.getState()));
                    for (StackTraceElement el : elements) {
                        result.append(String.format("%-40s %s\n", threadName, el.toString()));
                    }
                    result.append("\n");
                }
            }
        } catch (Throwable ignored) {
        }

        return result.toString();
    }

    public static String jstack(final String threadName, final long threadId) {
        Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
        StringBuilder result = new StringBuilder();
        try {
            for (final Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    if (threadName.equals(entry.getKey().getName()) && threadId == entry.getKey().getId()) {
                        result.append(String.format("%-40s TID: %d STATE: %s\n", threadName, thread.getId(), thread.getState()));
                        for (StackTraceElement el : elements) {
                            result.append(String.format("%-40s %s\n", threadName, el.toString()));
                        }
                    }
                }
            }
        } catch (Throwable ignored) {
        }
        return result.toString();
    }

    public static ExecuteResult callShellCommand(final String shellString) {
        Process process = null;
        try {
            String[] cmdArray = shellString.split(" ");
            process = Runtime.getRuntime().exec(cmdArray);
            process.waitFor();
        } catch (Throwable ignored) {
        } finally {
            if (null != process)
                process.destroy();
        }

        return null;
    }
}
