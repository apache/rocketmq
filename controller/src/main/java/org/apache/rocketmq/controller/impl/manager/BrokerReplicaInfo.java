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

import java.io.Serializable;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker replicas info, mapping from brokerAddress to {brokerId, brokerHaAddress}.
 */
public class BrokerReplicaInfo implements Serializable {
    private final String clusterName;

    private final String brokerName;

    // Start from 1
    private final AtomicLong nextAssignBrokerId;

    private final Map<Long/*brokerId*/, Pair<String/*ipAddress*/, String/*registerCheckCode*/>> brokerIdInfo;

    public BrokerReplicaInfo(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.nextAssignBrokerId = new AtomicLong(MixAll.FIRST_BROKER_CONTROLLER_ID);
        this.brokerIdInfo = new ConcurrentHashMap<>();
    }

    public void removeBrokerId(final Long brokerId) {
        this.brokerIdInfo.remove(brokerId);
    }

    public Long getNextAssignBrokerId() {
        return nextAssignBrokerId.get();
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void addBroker(final Long brokerId, final String ipAddress, final String registerCheckCode) {
        this.brokerIdInfo.put(brokerId, new Pair<>(ipAddress, registerCheckCode));
        this.nextAssignBrokerId.incrementAndGet();
    }

    public boolean isBrokerExist(final Long brokerId) {
        return this.brokerIdInfo.containsKey(brokerId);
    }

    public Set<Long> getAllBroker() {
        return new HashSet<>(this.brokerIdInfo.keySet());
    }

    public Map<Long, String> getBrokerIdTable() {
        Map<Long/*brokerId*/, String/*address*/> map = new HashMap<>(this.brokerIdInfo.size());
        this.brokerIdInfo.forEach((id, pair) -> {
            map.put(id, pair.getObject1());
        });
        return map;
    }

    public String getBrokerAddress(final Long brokerId) {
        if (brokerId == null) {
            return null;
        }
        Pair<String, String> pair = this.brokerIdInfo.get(brokerId);
        if (pair != null) {
            return pair.getObject1();
        }
        return null;
    }

    public String getBrokerRegisterCheckCode(final Long brokerId) {
        if (brokerId == null) {
            return null;
        }
        Pair<String, String> pair = this.brokerIdInfo.get(brokerId);
        if (pair != null) {
            return pair.getObject2();
        }
        return null;
    }

    public void updateBrokerAddress(final Long brokerId, final String brokerAddress) {
        if (brokerId == null)
            return;
        Pair<String, String> oldPair = this.brokerIdInfo.get(brokerId);
        if (oldPair != null) {
            this.brokerIdInfo.put(brokerId, new Pair<>(brokerAddress, oldPair.getObject2()));
        }
    }
}
