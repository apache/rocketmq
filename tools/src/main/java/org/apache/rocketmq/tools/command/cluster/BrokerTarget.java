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

import java.util.Comparator;

public class BrokerTarget {
    public static final long UNKNOWN_BROKER_ID = -1L;
    public static final String DIRECT_CLUSTER = "direct";
    public static final String DIRECT_BROKER = "direct";

    public static final Comparator<BrokerTarget> COMPARATOR = Comparator
        .comparing(BrokerTarget::getClusterName)
        .thenComparing(BrokerTarget::getBrokerName)
        .thenComparingLong(BrokerTarget::getBrokerId)
        .thenComparing(BrokerTarget::getBrokerAddr);

    private final String clusterName;
    private final String brokerName;
    private final long brokerId;
    private final String brokerAddr;

    public BrokerTarget(String clusterName, String brokerName, long brokerId, String brokerAddr) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.brokerAddr = brokerAddr;
    }

    public static BrokerTarget direct(String brokerAddr) {
        return new BrokerTarget(DIRECT_CLUSTER, DIRECT_BROKER, UNKNOWN_BROKER_ID, brokerAddr);
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }
}
