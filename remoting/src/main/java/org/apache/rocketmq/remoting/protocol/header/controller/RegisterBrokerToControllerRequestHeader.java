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
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RegisterBrokerToControllerRequestHeader implements CommandCustomHeader {
    private String clusterName;
    private String brokerName;
    private String brokerAddress;
    @CFNullable
    private Integer epoch;
    @CFNullable
    private Long maxOffset;
    @CFNullable
    private Long confirmOffset;
    @CFNullable
    private Long heartbeatTimeoutMillis;
    @CFNullable
    private Integer electionPriority;

    public RegisterBrokerToControllerRequestHeader() {
    }

    public RegisterBrokerToControllerRequestHeader(String clusterName, String brokerName, String brokerAddress) {
        this(clusterName, brokerName, brokerAddress, 0);
    }

    public RegisterBrokerToControllerRequestHeader(String clusterName, String brokerName, String brokerAddress,
        int electionPriority) {
        this(clusterName, brokerName, brokerAddress, null, 0, 0, electionPriority);
    }

    public RegisterBrokerToControllerRequestHeader(String clusterName, String brokerName, String brokerAddress,
        Long heartbeatTimeoutMillis, int epoch, long maxOffset, int electionPriority) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.epoch = epoch;
        this.maxOffset = maxOffset;
        this.electionPriority = electionPriority;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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

    public Long getHeartbeatTimeoutMillis() {
        return heartbeatTimeoutMillis;
    }

    public void setHeartbeatTimeoutMillis(Long heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    public Integer getElectionPriority() {
        return electionPriority;
    }

    public void setElectionPriority(Integer electionPriority) {
        this.electionPriority = electionPriority;
    }

    @Override
    public String toString() {
        return "RegisterBrokerToControllerRequestHeader{" +
            "clusterName='" + clusterName + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerAddress='" + brokerAddress + '\'' +
            ", epoch=" + epoch +
            ", maxOffset=" + maxOffset +
            ", confirmOffset=" + confirmOffset +
            ", heartbeatTimeoutMillis=" + heartbeatTimeoutMillis +
            ", electionPriority=" + electionPriority +
            '}';
    }

    public Integer getEpoch() {
        return epoch;
    }

    public void setEpoch(Integer epoch) {
        this.epoch = epoch;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public Long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(Long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
