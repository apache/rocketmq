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

package org.apache.rocketmq.controller.dledger.event.read;

import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;

public class GetReplicaInfoResult implements ReadEventResult {

    private Long masterBrokerId;
    private String masterAddress;
    private Integer masterEpoch;

    private SyncStateSet syncStateSet;

    public GetReplicaInfoResult(Long masterBrokerId, String masterAddress, Integer masterEpoch, SyncStateSet syncStateSet) {
        this.masterBrokerId = masterBrokerId;
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSet = syncStateSet;
    }

    public SyncStateSet getSyncStateSet() {
        return syncStateSet;
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    public void setMasterBrokerId(Long masterBrokerId) {
        this.masterBrokerId = masterBrokerId;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public Integer getMasterEpoch() {
        return masterEpoch;
    }

    public void setMasterEpoch(Integer masterEpoch) {
        this.masterEpoch = masterEpoch;
    }

    @Override
    public ReadEventType getEventType() {
        return ReadEventType.GET_REPLICA_INFO;
    }
}
