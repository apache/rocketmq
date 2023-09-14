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

package org.apache.rocketmq.remoting.protocol.body;

import com.google.common.base.Objects;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class BrokerMemberGroup extends RemotingSerializable {
    private String cluster;
    private String brokerName;
    private Map<Long/* brokerId */, String/* broker address */> brokerAddrs;

    // Provide default constructor for serializer
    public BrokerMemberGroup() {
        this.brokerAddrs = new HashMap<>();
    }

    public BrokerMemberGroup(final String cluster, final String brokerName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = new HashMap<>();
    }

    public long minimumBrokerId() {
        if (this.brokerAddrs.isEmpty()) {
            return 0;
        }
        return Collections.min(brokerAddrs.keySet());
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(final String cluster) {
        this.cluster = cluster;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(final String brokerName) {
        this.brokerName = brokerName;
    }

    public Map<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(final Map<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BrokerMemberGroup that = (BrokerMemberGroup) o;
        return Objects.equal(cluster, that.cluster) &&
            Objects.equal(brokerName, that.brokerName) &&
            Objects.equal(brokerAddrs, that.brokerAddrs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cluster, brokerName, brokerAddrs);
    }

    @Override
    public String toString() {
        return "BrokerMemberGroup{" +
            "cluster='" + cluster + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerAddrs=" + brokerAddrs +
            '}';
    }
}
