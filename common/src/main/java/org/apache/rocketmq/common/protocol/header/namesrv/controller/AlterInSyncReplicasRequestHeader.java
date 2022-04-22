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
package org.apache.rocketmq.common.protocol.header.namesrv.controller;

import java.util.Set;

public class AlterInSyncReplicasRequestHeader {
    private String brokerName;
    private String masterAddress;
    private int masterEpoch;
    private Set<String> newSyncStateSet;
    private int syncStateSetEpoch;

    public AlterInSyncReplicasRequestHeader(String brokerName, String masterAddress, int masterEpoch,
        Set<String> newSyncStateSet, int syncStateSetEpoch) {
        this.brokerName = brokerName;
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.newSyncStateSet = newSyncStateSet;
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
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

    public Set<String> getNewSyncStateSet() {
        return newSyncStateSet;
    }

    public void setNewSyncStateSet(Set<String> newSyncStateSet) {
        this.newSyncStateSet = newSyncStateSet;
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(int syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    @Override public String toString() {
        return "AlterInSyncReplicasRequestHeader{" +
            "brokerName='" + brokerName + '\'' +
            ", masterAddress='" + masterAddress + '\'' +
            ", masterEpoch=" + masterEpoch +
            ", newSyncStateSet=" + newSyncStateSet +
            ", syncStateSetEpoch=" + syncStateSetEpoch +
            '}';
    }
}
