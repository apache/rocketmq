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
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SyncStateInfoTest {

    @Test
    public void testInitialState() {
        SyncStateInfo info = new SyncStateInfo("cluster", "broker-a");

        assertThat(info.getClusterName()).isEqualTo("cluster");
        assertThat(info.getBrokerName()).isEqualTo("broker-a");
        assertThat(info.getMasterEpoch()).isEqualTo(0);
        assertThat(info.getSyncStateSetEpoch()).isEqualTo(0);
        assertThat(info.getSyncStateSet()).isEmpty();
        assertThat(info.isFirstTimeForElect()).isTrue();
        assertThat(info.isMasterExist()).isFalse();
        assertThat(info.getMasterBrokerId()).isNull();
    }

    @Test
    public void testUpdateMasterInfoSetsMasterAndIncrementsEpoch() {
        SyncStateInfo info = new SyncStateInfo("cluster", "broker-a");

        info.updateMasterInfo(1L);
        assertThat(info.getMasterBrokerId()).isEqualTo(1L);
        assertThat(info.isMasterExist()).isTrue();
        assertThat(info.isFirstTimeForElect()).isFalse();
        assertThat(info.getMasterEpoch()).isEqualTo(1);

        info.updateMasterInfo(2L);
        assertThat(info.getMasterBrokerId()).isEqualTo(2L);
        assertThat(info.getMasterEpoch()).isEqualTo(2);
    }

    @Test
    public void testUpdateSyncStateSetIsolatedFromCallerMutation() {
        SyncStateInfo info = new SyncStateInfo("cluster", "broker-a");

        Set<Long> callerSet = new HashSet<>();
        callerSet.add(1L);
        callerSet.add(2L);
        info.updateSyncStateSetInfo(callerSet);
        assertThat(info.getSyncStateSetEpoch()).isEqualTo(1);

        // mutating the set passed to update must not change the recorded state
        callerSet.clear();

        assertThat(info.getSyncStateSet()).containsOnly(1L, 2L);
    }

    @Test
    public void testGetSyncStateSetIsolatedFromReturnedMutation() {
        SyncStateInfo info = new SyncStateInfo("cluster", "broker-a");

        Set<Long> initial = new HashSet<>();
        initial.add(1L);
        initial.add(2L);
        initial.add(3L);
        info.updateSyncStateSetInfo(initial);

        // mutating the returned set must not change the recorded state
        Set<Long> returned = info.getSyncStateSet();
        returned.clear();

        assertThat(info.getSyncStateSet()).containsOnly(1L, 2L, 3L);
    }

    @Test
    public void testRemoveFromSyncState() {
        SyncStateInfo info = new SyncStateInfo("cluster", "broker-a");

        Set<Long> initial = new HashSet<>();
        initial.add(1L);
        initial.add(2L);
        initial.add(3L);
        info.updateSyncStateSetInfo(initial);

        info.removeFromSyncState(2L);

        assertThat(info.getSyncStateSet()).containsOnly(1L, 3L);
        // removal is a local adjustment; it does not advance the sync-state epoch
        assertThat(info.getSyncStateSetEpoch()).isEqualTo(1);
    }
}
