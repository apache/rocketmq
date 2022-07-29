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

import org.apache.rocketmq.common.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class ElectMasterResponseHeader implements CommandCustomHeader {
    private String newMasterAddress;
    private int masterEpoch;
    private int syncStateSetEpoch;
    private BrokerMemberGroup brokerMemberGroup;

    public ElectMasterResponseHeader() {
    }

    public String getNewMasterAddress() {
        return newMasterAddress;
    }

    public void setNewMasterAddress(String newMasterAddress) {
        this.newMasterAddress = newMasterAddress;
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

    public BrokerMemberGroup getBrokerMemberGroup() {
        return brokerMemberGroup;
    }

    public void setBrokerMemberGroup(BrokerMemberGroup brokerMemberGroup) {
        this.brokerMemberGroup = brokerMemberGroup;
    }

    @Override
    public String toString() {
        return "ElectMasterResponseHeader{" +
            "newMasterAddress='" + newMasterAddress + '\'' +
            ", masterEpoch=" + masterEpoch +
            ", syncStateSetEpoch=" + syncStateSetEpoch +
            ", brokerMember=" + brokerMemberGroup +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
