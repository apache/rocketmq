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

package org.apache.rocketmq.controller.dledger.statemachine.event.read;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.controller.dledger.manager.BrokerReplicaInfo;
import org.apache.rocketmq.controller.dledger.manager.SyncStateInfo;

public class GetSyncStateSetResult implements ReadEventResult {

    private Map<String/*broker name*/, Pair<BrokerReplicaInfo, SyncStateInfo>> brokerSyncStateInfoMap = new HashMap<>();

    public GetSyncStateSetResult() {
    }

    public void addBrokerSyncStateInfo(String brokerName, BrokerReplicaInfo brokerReplicaInfo, SyncStateInfo syncStateInfo) {
        brokerSyncStateInfoMap.put(brokerName, new Pair<>(brokerReplicaInfo, syncStateInfo));
    }

    public Map<String, Pair<BrokerReplicaInfo, SyncStateInfo>> getBrokerSyncStateInfoMap() {
        return brokerSyncStateInfoMap;
    }

    @Override
    public ReadEventType getEventType() {
        return ReadEventType.GET_SYNC_STATE_SET;
    }

}
