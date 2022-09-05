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
package org.apache.rocketmq.controller.impl.manager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker info, mapping from brokerAddress to {brokerId, brokerHaAddress}.
 */
public class BrokerInfo {
    private final String clusterName;
    private final String brokerName;
    // Start from 1
    private final AtomicLong brokerIdCount;
    private final HashMap<String/*Address*/, Long/*brokerId*/> brokerIdTable;

    public BrokerInfo(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerIdCount = new AtomicLong(1L);
        this.brokerIdTable = new HashMap<>();
    }

    public void removeBrokerAddress(final String address) {
        this.brokerIdTable.remove(address);
    }

    public long newBrokerId() {
        return this.brokerIdCount.incrementAndGet();
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void addBroker(final String address, final Long brokerId) {
        this.brokerIdTable.put(address, brokerId);
    }

    public boolean isBrokerExist(final String address) {
        return this.brokerIdTable.containsKey(address);
    }

    public Set<String> getAllBroker() {
        return new HashSet<>(this.brokerIdTable.keySet());
    }

    public HashMap<String, Long> getBrokerIdTable() {
        return new HashMap<>(this.brokerIdTable);
    }

    public Long getBrokerId(final String address) {
        if (this.brokerIdTable.containsKey(address)) {
            return this.brokerIdTable.get(address);
        }
        return -1L;
    }
}
