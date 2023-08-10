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

package org.apache.rocketmq.controller.heartbeat;

import java.util.Objects;

public class BrokerSetIdentity {
    private String clusterName;
    private String brokerName;

    public BrokerSetIdentity(String clusterName, String brokerName) {
        this.clusterName = clusterName;
        this.brokerName = brokerName;
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

    @Override
    public String toString() {
        return "BrokerSetIdentity{" +
            "clusterName='" + clusterName + '\'' +
            ", brokerName='" + brokerName + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DefaultBrokerHeartbeatManager.BrokerSetIdentity identity = (DefaultBrokerHeartbeatManager.BrokerSetIdentity) o;
        return Objects.equals(clusterName, identity.clusterName) && Objects.equals(brokerName, identity.brokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, brokerName);
    }
}
