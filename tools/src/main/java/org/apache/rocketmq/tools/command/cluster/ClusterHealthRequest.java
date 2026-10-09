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
package org.apache.rocketmq.tools.command.cluster;

public class ClusterHealthRequest {
    public static final long DEFAULT_TIMEOUT_MILLIS = 3000L;
    public static final int DEFAULT_PARALLELISM = 4;
    public static final int MAX_PARALLELISM = 64;

    private String brokerAddr;
    private String clusterName;
    private boolean namesrvOnly;
    private boolean mastersOnly;
    private boolean requireActive;
    private long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    private int parallelism = DEFAULT_PARALLELISM;

    public void validate() {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be greater than zero");
        }
        if (parallelism <= 0 || parallelism > MAX_PARALLELISM) {
            throw new IllegalArgumentException("parallelism must be between 1 and " + MAX_PARALLELISM);
        }
        if (hasText(brokerAddr) && hasText(clusterName)) {
            throw new IllegalArgumentException("brokerAddr and clusterName cannot be used together");
        }
        if (hasText(brokerAddr) && namesrvOnly) {
            throw new IllegalArgumentException("brokerAddr and namesrvOnly cannot be used together");
        }
        if (namesrvOnly && mastersOnly) {
            throw new IllegalArgumentException("mastersOnly does not apply to a NameServer-only check");
        }
        if (namesrvOnly && requireActive) {
            throw new IllegalArgumentException("requireActive does not apply to a NameServer-only check");
        }
    }

    public boolean isDirectBrokerCheck() {
        return hasText(brokerAddr);
    }

    public String describeTarget() {
        if (isDirectBrokerCheck()) {
            return "broker:" + brokerAddr;
        }
        if (namesrvOnly) {
            return "nameserver";
        }
        if (hasText(clusterName)) {
            return "cluster:" + clusterName;
        }
        return "all-clusters";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = normalize(brokerAddr);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = normalize(clusterName);
    }

    public boolean isNamesrvOnly() {
        return namesrvOnly;
    }

    public void setNamesrvOnly(boolean namesrvOnly) {
        this.namesrvOnly = namesrvOnly;
    }

    public boolean isMastersOnly() {
        return mastersOnly;
    }

    public void setMastersOnly(boolean mastersOnly) {
        this.mastersOnly = mastersOnly;
    }

    public boolean isRequireActive() {
        return requireActive;
    }

    public void setRequireActive(boolean requireActive) {
        this.requireActive = requireActive;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    private static String normalize(String value) {
        return value == null ? null : value.trim();
    }
}
