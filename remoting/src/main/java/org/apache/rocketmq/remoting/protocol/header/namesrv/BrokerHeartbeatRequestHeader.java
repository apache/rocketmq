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

package org.apache.rocketmq.remoting.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class BrokerHeartbeatRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String clusterName;
    @CFNotNull
    private String brokerAddr;
    @CFNotNull
    private String brokerName;
    @CFNullable
    private Long brokerId;
    @CFNullable
    private Integer epoch;
    @CFNullable
    private Long maxOffset;
    @CFNullable
    private Long confirmOffset;
    @CFNullable
    private Long heartbeatTimeoutMills;
    @CFNullable
    private Integer electionPriority;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
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

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public Long getHeartbeatTimeoutMills() {
        return heartbeatTimeoutMills;
    }

    public void setHeartbeatTimeoutMills(Long heartbeatTimeoutMills) {
        this.heartbeatTimeoutMills = heartbeatTimeoutMills;
    }

    public Integer getElectionPriority() {
        return electionPriority;
    }

    public void setElectionPriority(Integer electionPriority) {
        this.electionPriority = electionPriority;
    }
}
