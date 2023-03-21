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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the syncStateSet of broker replicas.
 */
public class SyncStateInfo {
    private final String clusterName;
    private final String brokerName;
    private final AtomicInteger masterEpoch;
    private final AtomicInteger syncStateSetEpoch;

    private Set<Long/*brokerId*/> syncStateSet;

    private Long masterBrokerId;

    public SyncStateInfo(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.masterEpoch = new AtomicInteger(0);
        this.syncStateSetEpoch = new AtomicInteger(0);
        this.syncStateSet = Collections.emptySet();
    }

    public void updateMasterInfo(Long masterBrokerId) {
        this.masterBrokerId = masterBrokerId;
        this.masterEpoch.incrementAndGet();
    }

    public void updateSyncStateSetInfo(Set<Long> newSyncStateSet) {
        this.syncStateSet = new HashSet<>(newSyncStateSet);
        this.syncStateSetEpoch.incrementAndGet();
    }

    public boolean isFirstTimeForElect() {
        return this.masterEpoch.get() == 0;
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
        return syncStateSetEpoch.get();
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    public int getMasterEpoch() {
        return masterEpoch.get();
    }

    public void removeFromSyncState(final Long brokerId) {
        syncStateSet.remove(brokerId);
    }
}
