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
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class AlterSyncStateSetResponseHeader implements CommandCustomHeader {
    private short errorCode = ErrorCodes.NONE.getCode();
    private Set<String> newSyncStateSet;
    private int newSyncStateSetEpoch;

    public AlterSyncStateSetResponseHeader() {
    }

    public AlterSyncStateSetResponseHeader(Set<String> newSyncStateSet, int newSyncStateSetEpoch) {
        this.newSyncStateSet = newSyncStateSet;
        this.newSyncStateSetEpoch = newSyncStateSetEpoch;
    }

    public short getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(short errorCode) {
        this.errorCode = errorCode;
    }

    public Set<String> getNewSyncStateSet() {
        return newSyncStateSet;
    }

    public void setNewSyncStateSet(Set<String> newSyncStateSet) {
        this.newSyncStateSet = newSyncStateSet;
    }

    public int getNewSyncStateSetEpoch() {
        return newSyncStateSetEpoch;
    }

    public void setNewSyncStateSetEpoch(int newSyncStateSetEpoch) {
        this.newSyncStateSetEpoch = newSyncStateSetEpoch;
    }

    @Override
    public String toString() {
        return "AlterSyncStateSetResponseHeader{" +
            "errorCode=" + errorCode +
            ", newSyncStateSet=" + newSyncStateSet +
            ", newSyncStateSetEpoch=" + newSyncStateSetEpoch +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
