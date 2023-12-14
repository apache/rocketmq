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
package org.apache.rocketmq.controller.impl.heartbeat;

import io.netty.channel.Channel;
import java.io.Serializable;

public class BrokerLiveInfo implements Serializable {
    private static final long serialVersionUID = 3612173344946510993L;
    private final String brokerName;

    private String brokerAddr;
    private long heartbeatTimeoutMillis;
    private Channel channel;
    private long brokerId;
    private long lastUpdateTimestamp;
    private int epoch;
    private long maxOffset;
    private long confirmOffset;
    private Integer electionPriority;

    public BrokerLiveInfo(String brokerName, String brokerAddr, long brokerId, long lastUpdateTimestamp,
        long heartbeatTimeoutMillis, Channel channel, int epoch, long maxOffset, Integer electionPriority) {
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;
        this.brokerId = brokerId;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.channel = channel;
        this.epoch = epoch;
        this.electionPriority = electionPriority;
        this.maxOffset = maxOffset;
    }

    public BrokerLiveInfo(String brokerName, String brokerAddr, long brokerId, long lastUpdateTimestamp,
        long heartbeatTimeoutMillis, Channel channel, int epoch, long maxOffset, Integer electionPriority,
        long confirmOffset) {
        this.brokerName = brokerName;
        this.brokerAddr = brokerAddr;
        this.brokerId = brokerId;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.channel = channel;
        this.epoch = epoch;
        this.maxOffset = maxOffset;
        this.electionPriority = electionPriority;
        this.confirmOffset = confirmOffset;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo{" +
            "brokerName='" + brokerName + '\'' +
            ", brokerAddr='" + brokerAddr + '\'' +
            ", heartbeatTimeoutMillis=" + heartbeatTimeoutMillis +
            ", channel=" + channel +
            ", brokerId=" + brokerId +
            ", lastUpdateTimestamp=" + lastUpdateTimestamp +
            ", epoch=" + epoch +
            ", maxOffset=" + maxOffset +
            ", confirmOffset=" + confirmOffset +
            '}';
    }

    public String getBrokerName() {
        return brokerName;
    }

    public long getHeartbeatTimeoutMillis() {
        return heartbeatTimeoutMillis;
    }

    public void setHeartbeatTimeoutMillis(long heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    public Channel getChannel() {
        return channel;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public void setElectionPriority(Integer electionPriority) {
        this.electionPriority = electionPriority;
    }

    public Integer getElectionPriority() {
        return electionPriority;
    }

    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
