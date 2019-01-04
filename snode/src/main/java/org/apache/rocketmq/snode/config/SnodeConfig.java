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
package org.apache.rocketmq.snode.config;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import static org.apache.rocketmq.client.ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY;

public class SnodeConfig {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    private String snodeName = "defaultNode";

    private String snodeAddr = "127.0.0.1:11911";

    private String clusterName = "defaultCluster";

    private int snodeSendThreadPoolQueueCapacity = 10000;

    private int snodeSendMessageMinPoolSize = 10;

    private int snodeSendMessageMaxPoolSize = 20;

    private int snodeHeartBeatCorePoolSize = 1;

    private int snodeHeartBeatMaxPoolSize = 2;

    private int snodeHeartBeatThreadPoolQueueCapacity = 1000;

    private long snodeHeartBeatInterval = 30 * 1000;

    private boolean fetechNameserver = false;

    private long houseKeepingInterval = 10 * 1000;

    private boolean notifyConsumerIdsChangedEnable = true;

    private boolean autoCreateSubscriptionGroup = true;

    private int snodePushMessageMinPoolSize = 10;

    private int snodePushMessageMaxPoolSize = 20;

    private int snodePushMessageThreadPoolQueueCapacity = 10000;


    private int listenPort = 11911;

    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "true"));

    public void setSnodeHeartBeatInterval(long snodeHeartBeatInterval) {
        this.snodeHeartBeatInterval = snodeHeartBeatInterval;
    }

    public long getHouseKeepingInterval() {
        return houseKeepingInterval;
    }

    public void setHouseKeepingInterval(long houseKeepingInterval) {
        this.houseKeepingInterval = houseKeepingInterval;
    }

    public boolean isFetechNameserver() {
        return fetechNameserver;
    }

    public void setFetechNameserver(boolean fetechNameserver) {
        this.fetechNameserver = fetechNameserver;
    }

    public long getSnodeHeartBeatInterval() {
        return snodeHeartBeatInterval;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10, 000 and 60, 000 milliseconds.
     */
    private int registerNameServerPeriod = 1000 * 30;

    public int getRegisterNameServerPeriod() {
        return registerNameServerPeriod;
    }

    public void setRegisterNameServerPeriod(int registerNameServerPeriod) {
        this.registerNameServerPeriod = registerNameServerPeriod;
    }

    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public int getSnodeHeartBeatThreadPoolQueueCapacity() {
        return snodeHeartBeatThreadPoolQueueCapacity;
    }

    public void setSnodeHeartBeatThreadPoolQueueCapacity(int snodeHeartBeatThreadPoolQueueCapacity) {
        this.snodeHeartBeatThreadPoolQueueCapacity = snodeHeartBeatThreadPoolQueueCapacity;
    }

    public int getSnodeHeartBeatCorePoolSize() {
        return snodeHeartBeatCorePoolSize;
    }

    public void setSnodeHeartBeatCorePoolSize(int snodeHeartBeatCorePoolSize) {
        this.snodeHeartBeatCorePoolSize = snodeHeartBeatCorePoolSize;
    }

    public int getSnodeHeartBeatMaxPoolSize() {
        return snodeHeartBeatMaxPoolSize;
    }

    public void setSnodeHeartBeatMaxPoolSize(int snodeHeartBeatMaxPoolSize) {
        this.snodeHeartBeatMaxPoolSize = snodeHeartBeatMaxPoolSize;
    }

    public int getListenPort() {
        return listenPort;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getSnodeSendThreadPoolQueueCapacity() {
        return snodeSendThreadPoolQueueCapacity;
    }

    public void setSnodeSendThreadPoolQueueCapacity(int snodeSendThreadPoolQueueCapacity) {
        this.snodeSendThreadPoolQueueCapacity = snodeSendThreadPoolQueueCapacity;
    }

    public int getSnodeSendMessageMinPoolSize() {
        return snodeSendMessageMinPoolSize;
    }

    public void setSnodeSendMessageMinPoolSize(int snodeSendMessageMinPoolSize) {
        this.snodeSendMessageMinPoolSize = snodeSendMessageMinPoolSize;
    }

    public int getSnodeSendMessageMaxPoolSize() {
        return snodeSendMessageMaxPoolSize;
    }

    public void setSnodeSendMessageMaxPoolSize(int snodeSendMessageMaxPoolSize) {
        this.snodeSendMessageMaxPoolSize = snodeSendMessageMaxPoolSize;
    }

    public String getSnodeAddr() {
        return snodeAddr;
    }

    public void setSnodeAddr(String snodeAddr) {
        this.snodeAddr = snodeAddr;
    }

    public String getSnodeName() {
        return snodeName;
    }

    public void setSnodeName(String snodeName) {
        this.snodeName = snodeName;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public int getSnodePushMessageMinPoolSize() {
        return snodePushMessageMinPoolSize;
    }

    public void setSnodePushMessageMinPoolSize(int snodePushMessageMinPoolSize) {
        this.snodePushMessageMinPoolSize = snodePushMessageMinPoolSize;
    }

    public int getSnodePushMessageMaxPoolSize() {
        return snodePushMessageMaxPoolSize;
    }

    public void setSnodePushMessageMaxPoolSize(int snodePushMessageMaxPoolSize) {
        this.snodePushMessageMaxPoolSize = snodePushMessageMaxPoolSize;
    }

    public int getSnodePushMessageThreadPoolQueueCapacity() {
        return snodePushMessageThreadPoolQueueCapacity;
    }

    public void setSnodePushMessageThreadPoolQueueCapacity(int snodePushMessageThreadPoolQueueCapacity) {
        this.snodePushMessageThreadPoolQueueCapacity = snodePushMessageThreadPoolQueueCapacity;
    }
}
