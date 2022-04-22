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

public class GetReplicaInfoResponseHeader {
    private short errorCode = ErrorCodes.NONE.getCode();
    private String masterAddress;
    private int masterEpoch;
    private Set<String> syncStateSet;
    private int syncStateSetEpoch;

    public GetReplicaInfoResponseHeader() {
    }

    public GetReplicaInfoResponseHeader(String masterAddress, int masterEpoch, Set<String> syncStateSet,
        int syncStateSetEpoch) {
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSet = syncStateSet;
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
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

    public Set<String> getSyncStateSet() {
        return syncStateSet;
    }

    public void setSyncStateSet(Set<String> syncStateSet) {
        this.syncStateSet = syncStateSet;
    }

    public int getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(int syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    @Override public String toString() {
        return "GetReplicaInfoResponseHeader{" +
            "errorCode=" + errorCode +
            ", masterAddress='" + masterAddress + '\'' +
            ", masterEpoch=" + masterEpoch +
            ", syncStateSet=" + syncStateSet +
            ", syncStateSetEpoch=" + syncStateSetEpoch +
            '}';
    }
}
