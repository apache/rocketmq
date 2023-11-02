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

package org.apache.rocketmq.container;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.utils.NetworkUtil;

public class BrokerContainerConfig {

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    @ImportantField
    private boolean fetchNameSrvAddrByDnsLookup = false;

    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    @ImportantField
    private String brokerContainerIP = NetworkUtil.getLocalAddress();

    private String brokerConfigPaths = null;
    
    /**
     * The interval to fetch namesrv addr, default value is 10 second
     */
    private long fetchNamesrvAddrInterval = 10 * 1000;

    /**
     * The interval to update namesrv addr, default value is 120 second
     */
    private long updateNamesrvAddrInterval = 60 * 2 * 1000;

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public boolean isFetchNameSrvAddrByDnsLookup() {
        return fetchNameSrvAddrByDnsLookup;
    }

    public void setFetchNameSrvAddrByDnsLookup(boolean fetchNameSrvAddrByDnsLookup) {
        this.fetchNameSrvAddrByDnsLookup = fetchNameSrvAddrByDnsLookup;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public String getBrokerContainerIP() {
        return brokerContainerIP;
    }

    public String getBrokerConfigPaths() {
        return brokerConfigPaths;
    }

    public void setBrokerConfigPaths(String brokerConfigPaths) {
        this.brokerConfigPaths = brokerConfigPaths;
    }
    
    public long getFetchNamesrvAddrInterval() {
        return fetchNamesrvAddrInterval;
    }
    
    public void setFetchNamesrvAddrInterval(final long fetchNamesrvAddrInterval) {
        this.fetchNamesrvAddrInterval = fetchNamesrvAddrInterval;
    }

    public long getUpdateNamesrvAddrInterval() {
        return updateNamesrvAddrInterval;
    }

    public void setUpdateNamesrvAddrInterval(long updateNamesrvAddrInterval) {
        this.updateNamesrvAddrInterval = updateNamesrvAddrInterval;
    }
}
