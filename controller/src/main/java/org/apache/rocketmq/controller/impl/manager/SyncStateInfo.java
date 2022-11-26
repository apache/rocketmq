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

import com.alibaba.fastjson.JSONArray;

import java.util.HashSet;
import java.util.Set;

/**
 * Manages the syncStateSet of broker replicas.
 */
public class SyncStateInfo {
    private String clusterName;
    private String brokerName;

    private String masterAddress;
    private int masterEpoch;

    private int syncStateSetEpoch;
    private Set<String/*Address*/> syncStateSet;

    public SyncStateInfo(String clusterName, String brokerName, String masterAddress) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterAddress = masterAddress;
        this.masterEpoch = 1;
        this.syncStateSet = new HashSet<>();
        this.syncStateSet.add(masterAddress);
        this.syncStateSetEpoch = 1;
    }

    // This constructor is to adapt FastJSONSerializer.
    public SyncStateInfo(String clusterName, String brokerName, String masterAddress, int masterEpoch, int syncStateSetEpoch, JSONArray syncStateSet) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSetEpoch = syncStateSetEpoch;
        this.syncStateSet = new HashSet<>();
        syncStateSet.forEach(entry -> this.syncStateSet.add((String) entry));
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

    public void removeSyncState(final String address) {
        syncStateSet.remove(address);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public Set<String> getSyncStateSet() {
        return new HashSet<>(syncStateSet);
    }

    public void setSyncStateSet(Set<String> syncStateSet) {
        this.syncStateSet = syncStateSet;
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(int syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public void setMasterEpoch(int masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    @Override
    public String toString() {
        return "SyncStateInfo{" +
                "clusterName='" + clusterName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", masterAddress='" + masterAddress + '\'' +
                ", masterEpoch=" + masterEpoch +
                ", syncStateSetEpoch=" + syncStateSetEpoch +
                ", syncStateSet=" + syncStateSet +
                '}';
    }
}
