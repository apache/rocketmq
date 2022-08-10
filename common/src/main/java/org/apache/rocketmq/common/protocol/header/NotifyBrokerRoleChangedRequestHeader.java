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
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class NotifyBrokerRoleChangedRequestHeader implements CommandCustomHeader {
    private String masterAddress;
    private int masterEpoch;
    private int syncStateSetEpoch;
    // The id of this broker.
    private long brokerId;

    public NotifyBrokerRoleChangedRequestHeader() {
    }

    public NotifyBrokerRoleChangedRequestHeader(String masterAddress, int masterEpoch, int syncStateSetEpoch, long brokerId) {
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSetEpoch = syncStateSetEpoch;
        this.brokerId = brokerId;
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

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public String toString() {
        return "NotifyBrokerRoleChangedRequestHeader{" +
            "masterAddress='" + masterAddress + '\'' +
            ", masterEpoch=" + masterEpoch +
            ", syncStateSetEpoch=" + syncStateSetEpoch +
            ", brokerId=" + brokerId +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
