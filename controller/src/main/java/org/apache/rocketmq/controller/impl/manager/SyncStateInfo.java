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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Manages the syncStateSet of broker replicas.
 */
public class SyncStateInfo {
    private final String clusterName;
    private final String brokerName;

    private Set<Long/*brokerId*/> syncStateSet;
    private int syncStateSetEpoch;

    private Long masterBrokerId;
    private int masterEpoch;

    public SyncStateInfo(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterEpoch = 0;
        this.syncStateSetEpoch = 0;
        this.syncStateSet = Collections.emptySet();
    }


    public SyncStateInfo(String clusterName, String brokerName, Long masterBrokerId) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterBrokerId = masterBrokerId;
        this.masterEpoch = 1;
        this.syncStateSet = new HashSet<>();
        this.syncStateSet.add(masterBrokerId);
        this.syncStateSetEpoch = 1;
    }


    public void updateMasterInfo(Long masterBrokerId) {
        this.masterBrokerId = masterBrokerId;
        this.masterEpoch++;
    }

    public void updateSyncStateSetInfo(Set<Long> newSyncStateSet) {
        this.syncStateSet = new HashSet<>(newSyncStateSet);
        this.syncStateSetEpoch++;
    }

    public boolean isFirstTimeForElect() {
        return this.masterEpoch == 0;
    }

    public boolean isMasterExist() {
        return masterBrokerId != null;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public Set<Long> getSyncStateSet() {
        return new HashSet<>(syncStateSet);
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public void removeFromSyncState(final Long brokerId) {
        syncStateSet.remove(brokerId);
    }
}
