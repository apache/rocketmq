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
package org.apache.rocketmq.proxy.config;

public enum MetricCollectorMode {
    /**
     * Do not collect the metric from clients.
     */
    OFF("off"),
    /**
     * Collect the metric from clients to the given address.
     */
    ON("on"),
    /**
     * Collect the metric by the proxy itself.
     */
    PROXY("proxy");

    private final String modeString;

    MetricCollectorMode(String modeString) {
        this.modeString = modeString;
    }

    public String getModeString() {
        return modeString;
    }

    public static MetricCollectorMode getEnumByString(String modeString) {
        for (MetricCollectorMode mode : MetricCollectorMode.values()) {
            if (mode.modeString.equals(modeString.toLowerCase())) {
                return mode;
            }
        }
        return OFF;
    }
}
