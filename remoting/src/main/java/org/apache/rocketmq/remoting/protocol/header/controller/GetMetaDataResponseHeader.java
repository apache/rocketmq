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

public class GetMetaDataResponseHeader implements CommandCustomHeader {
    private String group;
    private String controllerLeaderId;
    private String controllerLeaderAddress;
    private boolean isLeader;
    private String peers;

    public GetMetaDataResponseHeader() {
    }

    public GetMetaDataResponseHeader(String group, String controllerLeaderId, String controllerLeaderAddress, boolean isLeader, String peers) {
        this.group = group;
        this.controllerLeaderId = controllerLeaderId;
        this.controllerLeaderAddress = controllerLeaderAddress;
        this.isLeader = isLeader;
        this.peers = peers;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getControllerLeaderId() {
        return controllerLeaderId;
    }

    public void setControllerLeaderId(String controllerLeaderId) {
        this.controllerLeaderId = controllerLeaderId;
    }

    public String getControllerLeaderAddress() {
        return controllerLeaderAddress;
    }

    public void setControllerLeaderAddress(String controllerLeaderAddress) {
        this.controllerLeaderAddress = controllerLeaderAddress;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void setIsLeader(boolean leader) {
        isLeader = leader;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    @Override
    public String toString() {
        return "GetMetaDataResponseHeader{" +
            "group='" + group + '\'' +
            ", controllerLeaderId='" + controllerLeaderId + '\'' +
            ", controllerLeaderAddress='" + controllerLeaderAddress + '\'' +
            ", isLeader=" + isLeader +
            ", peers='" + peers + '\'' +
            '}';
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
