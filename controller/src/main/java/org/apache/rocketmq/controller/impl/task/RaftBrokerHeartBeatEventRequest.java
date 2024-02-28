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
package org.apache.rocketmq.controller.impl.task;

import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RaftBrokerHeartBeatEventRequest implements CommandCustomHeader {
    // brokerIdentityInfo
    private String clusterNameIdentityInfo;

    private String brokerNameIdentityInfo;

    private Long brokerIdIdentityInfo;

    // brokerLiveInfo
    private String brokerName;
    private String brokerAddr;
    private Long heartbeatTimeoutMillis;
    private Long brokerId;
    private Long lastUpdateTimestamp;
    private Integer epoch;
    private Long maxOffset;
    private Long confirmOffset;
    private Integer electionPriority;

    public RaftBrokerHeartBeatEventRequest() {
    }

    public RaftBrokerHeartBeatEventRequest(BrokerIdentityInfo brokerIdentityInfo, BrokerLiveInfo brokerLiveInfo) {
        this.clusterNameIdentityInfo = brokerIdentityInfo.getClusterName();
        this.brokerNameIdentityInfo = brokerIdentityInfo.getBrokerName();
        this.brokerIdIdentityInfo = brokerIdentityInfo.getBrokerId();

        this.brokerName = brokerLiveInfo.getBrokerName();
        this.brokerAddr = brokerLiveInfo.getBrokerAddr();
        this.heartbeatTimeoutMillis = brokerLiveInfo.getHeartbeatTimeoutMillis();
        this.brokerId = brokerLiveInfo.getBrokerId();
        this.lastUpdateTimestamp = brokerLiveInfo.getLastUpdateTimestamp();
        this.epoch = brokerLiveInfo.getEpoch();
        this.maxOffset = brokerLiveInfo.getMaxOffset();
        this.confirmOffset = brokerLiveInfo.getConfirmOffset();
        this.electionPriority = brokerLiveInfo.getElectionPriority();
    }

    public BrokerIdentityInfo getBrokerIdentityInfo() {
        return new BrokerIdentityInfo(clusterNameIdentityInfo, brokerNameIdentityInfo, brokerIdIdentityInfo);
    }

    public void setBrokerIdentityInfo(BrokerIdentityInfo brokerIdentityInfo) {
        this.clusterNameIdentityInfo = brokerIdentityInfo.getClusterName();
        this.brokerNameIdentityInfo = brokerIdentityInfo.getBrokerName();
        this.brokerIdIdentityInfo = brokerIdentityInfo.getBrokerId();
    }

    public BrokerLiveInfo getBrokerLiveInfo() {
        return new BrokerLiveInfo(brokerName, brokerAddr, brokerId, lastUpdateTimestamp, heartbeatTimeoutMillis, null, epoch, maxOffset, electionPriority, confirmOffset);
    }

    public void setBrokerLiveInfo(BrokerLiveInfo brokerLiveInfo) {
        this.brokerName = brokerLiveInfo.getBrokerName();
        this.brokerAddr = brokerLiveInfo.getBrokerAddr();
        this.heartbeatTimeoutMillis = brokerLiveInfo.getHeartbeatTimeoutMillis();
        this.brokerId = brokerLiveInfo.getBrokerId();
        this.lastUpdateTimestamp = brokerLiveInfo.getLastUpdateTimestamp();
        this.epoch = brokerLiveInfo.getEpoch();
        this.maxOffset = brokerLiveInfo.getMaxOffset();
        this.confirmOffset = brokerLiveInfo.getConfirmOffset();
        this.electionPriority = brokerLiveInfo.getElectionPriority();
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    @Override
    public String toString() {
        return "RaftBrokerHeartBeatEventRequest{" +
            "brokerIdentityInfo=" + getBrokerIdentityInfo() +
            ", brokerLiveInfo=" + getBrokerLiveInfo() +
            "}";
    }
}
