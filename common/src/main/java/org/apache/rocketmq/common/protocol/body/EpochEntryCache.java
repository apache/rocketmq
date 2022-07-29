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
package org.apache.rocketmq.common.protocol.body;

import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class EpochEntryCache extends RemotingSerializable {
    private String clusterName;
    private String brokerName;
    private long brokerId;
    private List<EpochEntry> epochList;
    private long maxOffset;

    public EpochEntryCache(String clusterName, String brokerName, long brokerId, List<EpochEntry> epochList, long maxOffset) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.epochList = epochList;
        this.maxOffset = maxOffset;
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

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public List<EpochEntry> getEpochList() {
        return this.epochList;
    }

    public void setEpochList(List<EpochEntry> epochList) {
        this.epochList = epochList;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    @Override
    public String toString() {
        return "EpochEntryCache{" +
            "clusterName='" + clusterName + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerId=" + brokerId +
            ", epochList=" + epochList +
            ", maxOffset=" + maxOffset +
            '}';
    }
}
