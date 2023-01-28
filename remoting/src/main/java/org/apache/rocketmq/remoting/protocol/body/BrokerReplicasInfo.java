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
package org.apache.rocketmq.remoting.protocol.body;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class BrokerReplicasInfo extends RemotingSerializable  {
    private Map<String/*brokerName*/, ReplicasInfo> replicasInfoTable;

    public BrokerReplicasInfo() {
        this.replicasInfoTable = new HashMap<>();
    }

    public void addReplicaInfo(final String brokerName, final ReplicasInfo replicasInfo) {
        this.replicasInfoTable.put(brokerName, replicasInfo);
    }

    public Map<String, ReplicasInfo> getReplicasInfoTable() {
        return replicasInfoTable;
    }

    public void setReplicasInfoTable(
        Map<String, ReplicasInfo> replicasInfoTable) {
        this.replicasInfoTable = replicasInfoTable;
    }

    public static class ReplicasInfo extends RemotingSerializable {
        private String masterAddress;
        private int masterEpoch;
        private int syncStateSetEpoch;
        private List<ReplicaIdentity> inSyncReplicas;
        private List<ReplicaIdentity> notInSyncReplicas;

        public ReplicasInfo(String masterAddress, int masterEpoch, int syncStateSetEpoch,
            List<ReplicaIdentity> inSyncReplicas,
            List<ReplicaIdentity> notInSyncReplicas) {
            this.masterAddress = masterAddress;
            this.masterEpoch = masterEpoch;
            this.syncStateSetEpoch = syncStateSetEpoch;
            this.inSyncReplicas = inSyncReplicas;
            this.notInSyncReplicas = notInSyncReplicas;
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

        public int getSyncStateSetEpoch() {
            return syncStateSetEpoch;
        }

        public void setSyncStateSetEpoch(int syncStateSetEpoch) {
            this.syncStateSetEpoch = syncStateSetEpoch;
        }

        public List<ReplicaIdentity> getInSyncReplicas() {
            return inSyncReplicas;
        }

        public void setInSyncReplicas(
            List<ReplicaIdentity> inSyncReplicas) {
            this.inSyncReplicas = inSyncReplicas;
        }

        public List<ReplicaIdentity> getNotInSyncReplicas() {
            return notInSyncReplicas;
        }

        public void setNotInSyncReplicas(
            List<ReplicaIdentity> notInSyncReplicas) {
            this.notInSyncReplicas = notInSyncReplicas;
        }
    }

    public static class ReplicaIdentity extends RemotingSerializable {
        private String address;
        private Long brokerId;

        public ReplicaIdentity(String address, Long brokerId) {
            this.address = address;
            this.brokerId = brokerId;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public Long getBrokerId() {
            return brokerId;
        }

        public void setBrokerId(Long brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public String toString() {
            return "{" +
                "address='" + address + '\'' +
                ", brokerId=" + brokerId +
                '}';
        }
    }
}
