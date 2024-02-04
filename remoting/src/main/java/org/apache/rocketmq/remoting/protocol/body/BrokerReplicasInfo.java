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
import java.util.Objects;

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

        private Long masterBrokerId;

        private String masterAddress;
        private Integer masterEpoch;
        private Integer syncStateSetEpoch;
        private List<ReplicaIdentity> inSyncReplicas;
        private List<ReplicaIdentity> notInSyncReplicas;

        public ReplicasInfo(Long masterBrokerId, String masterAddress, int masterEpoch, int syncStateSetEpoch,
                            List<ReplicaIdentity> inSyncReplicas, List<ReplicaIdentity> notInSyncReplicas) {
            this.masterBrokerId = masterBrokerId;
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

        public void setMasterBrokerId(Long masterBrokerId) {
            this.masterBrokerId = masterBrokerId;
        }

        public Long getMasterBrokerId() {
            return masterBrokerId;
        }

        public boolean isExistInSync(String brokerName, Long brokerId, String brokerAddress) {
            return this.getInSyncReplicas().contains(new ReplicaIdentity(brokerName, brokerId, brokerAddress));
        }

        public boolean isExistInNotSync(String brokerName, Long brokerId, String brokerAddress) {
            return this.getNotInSyncReplicas().contains(new ReplicaIdentity(brokerName, brokerId, brokerAddress));
        }

        public boolean isExistInAllReplicas(String brokerName, Long brokerId, String brokerAddress) {
            return this.isExistInSync(brokerName, brokerId, brokerAddress) || this.isExistInNotSync(brokerName, brokerId, brokerAddress);
        }
    }

    public static class ReplicaIdentity extends RemotingSerializable {
        private String brokerName;
        private Long brokerId;

        private String brokerAddress;
        private Boolean alive;

        public ReplicaIdentity(String brokerName, Long brokerId, String brokerAddress) {
            this.brokerName = brokerName;
            this.brokerId = brokerId;
            this.brokerAddress = brokerAddress;
            this.alive = false;
        }

        public ReplicaIdentity(String brokerName, Long brokerId, String brokerAddress, Boolean alive) {
            this.brokerName = brokerName;
            this.brokerId = brokerId;
            this.brokerAddress = brokerAddress;
            this.alive = alive;
        }

        public String getBrokerName() {
            return brokerName;
        }

        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
        }

        public String getBrokerAddress() {
            return brokerAddress;
        }

        public void setBrokerAddress(String brokerAddress) {
            this.brokerAddress = brokerAddress;
        }

        public Long getBrokerId() {
            return brokerId;
        }

        public void setBrokerId(Long brokerId) {
            this.brokerId = brokerId;
        }

        public Boolean getAlive() {
            return alive;
        }

        public void setAlive(Boolean alive) {
            this.alive = alive;
        }

        @Override
        public String toString() {
            return "ReplicaIdentity{" +
                    "brokerName='" + brokerName + '\'' +
                    ", brokerId=" + brokerId +
                    ", brokerAddress='" + brokerAddress + '\'' +
                    ", alive=" + alive +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaIdentity that = (ReplicaIdentity) o;
            return brokerName.equals(that.brokerName) && brokerId.equals(that.brokerId) && brokerAddress.equals(that.brokerAddress);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerName, brokerId, brokerAddress);
        }
    }
}
