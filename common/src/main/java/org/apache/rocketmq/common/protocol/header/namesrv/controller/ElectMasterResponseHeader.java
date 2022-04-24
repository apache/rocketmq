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

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class ElectMasterResponseHeader implements CommandCustomHeader {
    private short errorCode = ErrorCodes.NONE.getCode();
    private String newMasterAddress;
    private int masterEpoch;

    public ElectMasterResponseHeader() {
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
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

    @Override
    public String toString() {
        return "ElectMasterResponseHeader{" +
            "errorCode=" + errorCode +
            ", newMasterAddress='" + newMasterAddress + '\'' +
            ", masterEpoch=" + masterEpoch +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
