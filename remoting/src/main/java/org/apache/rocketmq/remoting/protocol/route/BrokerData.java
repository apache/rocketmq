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

package org.apache.rocketmq.remoting.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

/**
 * The class describes that a typical broker cluster's (in replication) details: the cluster (in sharding) name
 * that it belongs to, and all the single instance information for this cluster.
 */
public class BrokerData implements Comparable<BrokerData> {
    private String cluster;
    private String brokerName;

    /**
     * The container that store the all single instances for the current broker replication cluster.
     * The key is the brokerId, and the value is the address of the single broker instance.
     */
    private HashMap<Long, String> brokerAddrs;
    private String zoneName;
    private final Random random = new Random();

    /**
     * Enable acting master or not, used for old version HA adaption,
     */
    private boolean enableActingMaster = false;

    public BrokerData() {

    }

    public BrokerData(BrokerData brokerData) {
        this.cluster = brokerData.cluster;
        this.brokerName = brokerData.brokerName;
        if (brokerData.brokerAddrs != null) {
            this.brokerAddrs = new HashMap<>(brokerData.brokerAddrs);
        }
        this.zoneName = brokerData.zoneName;
        this.enableActingMaster = brokerData.enableActingMaster;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs,
        boolean enableActingMaster) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs, boolean enableActingMaster,
        String zoneName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
        this.zoneName = zoneName;
    }

    /**
     * Selects a (preferably master) broker address from the registered list. If the master's address cannot be found, a
     * slave broker address is selected in a random manner.
     *
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String masterAddress = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (masterAddress == null) {
            List<String> addrs = new ArrayList<>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return masterAddress;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public boolean isEnableActingMaster() {
        return enableActingMaster;
    }

    public void setEnableActingMaster(boolean enableActingMaster) {
        this.enableActingMaster = enableActingMaster;
    }

    public String getZoneName() {
        return zoneName;
    }

    public void setZoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null) {
                return false;
            }
        } else if (!brokerAddrs.equals(other.brokerAddrs)) {
            return false;
        }
        return StringUtils.equals(brokerName, other.brokerName);
    }

    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + ", enableActingMaster=" + enableActingMaster + "]";
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
