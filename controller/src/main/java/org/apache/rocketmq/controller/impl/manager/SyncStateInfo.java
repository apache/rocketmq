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

import java.util.HashSet;
import java.util.Set;

/**
 * Manages the syncStateSet of broker replicas.
 */
public class SyncStateInfo {
    private final String clusterName;
    private final String brokerName;

    private Set<String/*Address*/> syncStateSet;
    private int syncStateSetEpoch;

    private String masterAddress;
    private int masterEpoch;

    public SyncStateInfo(String clusterName, String brokerName, String masterAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterAddress = masterAddress;
        this.masterEpoch = 1;
        this.syncStateSet = new HashSet<>();
        this.syncStateSet.add(masterAddress);
        this.syncStateSetEpoch = 1;
    }

    public void updateMasterInfo(String masterAddress) {
        this.masterAddress = masterAddress;
        this.masterEpoch++;
    }

    public void updateSyncStateSetInfo(Set<String> newSyncStateSet) {
        this.syncStateSet = new HashSet<>(newSyncStateSet);
        this.syncStateSetEpoch++;
    }

    public boolean isMasterExist() {
        return !this.masterAddress.isEmpty();
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Set<String> getSyncStateSet() {
        return new HashSet<>(syncStateSet);
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public void removeSyncState(final String address) {
        syncStateSet.remove(address);
    }
}
