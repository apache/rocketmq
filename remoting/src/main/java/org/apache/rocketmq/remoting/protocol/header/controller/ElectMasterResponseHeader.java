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
package org.apache.rocketmq.remoting.protocol.header.controller;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;


public class ElectMasterResponseHeader implements CommandCustomHeader {

    private Long masterBrokerId;
    private String masterAddress;
    private Integer masterEpoch;
    private Integer syncStateSetEpoch;

    public ElectMasterResponseHeader() {
    }

    public ElectMasterResponseHeader(Long masterBrokerId, String masterAddress, Integer masterEpoch, Integer syncStateSetEpoch) {
        this.masterBrokerId = masterBrokerId;
        this.masterAddress = masterAddress;
        this.masterEpoch = masterEpoch;
        this.syncStateSetEpoch = syncStateSetEpoch;
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

    public Integer getSyncStateSetEpoch() {
        return syncStateSetEpoch;
    }

    public void setSyncStateSetEpoch(Integer syncStateSetEpoch) {
        this.syncStateSetEpoch = syncStateSetEpoch;
    }

    public void setMasterBrokerId(Long masterBrokerId) {
        this.masterBrokerId = masterBrokerId;
    }

    public Long getMasterBrokerId() {
        return masterBrokerId;
    }

    @Override
    public String toString() {
        return "ElectMasterResponseHeader{" +
                "masterBrokerId=" + masterBrokerId +
                ", masterAddress='" + masterAddress + '\'' +
                ", masterEpoch=" + masterEpoch +
                ", syncStateSetEpoch=" + syncStateSetEpoch +
                '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
