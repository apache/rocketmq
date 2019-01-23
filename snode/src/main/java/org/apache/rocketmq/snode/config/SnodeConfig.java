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

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import static org.apache.rocketmq.client.ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY;

public class SnodeConfig {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    @ImportantField
    private String snodeIP1 = RemotingUtil.getLocalAddress();

    private String snodeIP2 = RemotingUtil.getLocalAddress();

    @ImportantField
    private String snodeName = localHostName();

    @ImportantField
    private long snodeId = MixAll.MASTER_ID;

    private String clusterName = "defaultCluster";

    private int snodeSendThreadPoolQueueCapacity = 10000;

    private int snodeHandleMqttThreadPoolQueueCapacity = 10000;

    private int snodeSendMessageMinPoolSize = 10;

    private int snodeSendMessageMaxPoolSize = 20;

    private int snodeHandleMqttMessageMinPoolSize = 10;

    private int snodeHandleMqttMessageMaxPoolSize = 20;

    private int snodeHeartBeatCorePoolSize = 1;

    private int snodeHeartBeatMaxPoolSize = 2;

    private int snodeHeartBeatThreadPoolQueueCapacity = 1000;

    private long snodeHeartBeatInterval = 30 * 1000;

    private boolean fetechNameServer = false;

    private long houseKeepingInterval = 10 * 1000;

    private boolean notifyConsumerIdsChangedEnable = true;

    private boolean autoCreateSubscriptionGroup = true;

    private int snodePushMessageMinPoolSize = 10;

    private int snodePushMessageMaxPoolSize = 20;

    private int snodePushMessageThreadPoolQueueCapacity = 10000;

    private int slowConsumerThreshold = 1024;

    private final String sendMessageInterceptorPath = "META-INF/service/org.apache.rocketmq.snode.interceptor.SendMessageInterceptor";

    private final String consumeMessageInterceptorPath = "META-INF/service/org.apache.rocketmq.snode.interceptor.ConsumeMessageInterceptor";

    private final String remotingServerInterceptorPath = "META-INF/service/org.apache.rocketmq.snode.interceptor.RemotingServerInterceptor";

    private int listenPort = 11911;

    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "true"));
    private boolean enablePropertyFilter = true;

    /**
     * Acl feature switch
     */
    @ImportantField
    private boolean aclEnable = false;

    public void setSnodeHeartBeatInterval(long snodeHeartBeatInterval) {
        this.snodeHeartBeatInterval = snodeHeartBeatInterval;
    }

    public long getHouseKeepingInterval() {
        return houseKeepingInterval;
    }

    public void setHouseKeepingInterval(long houseKeepingInterval) {
        this.houseKeepingInterval = houseKeepingInterval;
    }

    public boolean isFetechNameServer() {
        return fetechNameServer;
    }

    public void setFetechNameServer(boolean fetechNameServer) {
        this.fetechNameServer = fetechNameServer;
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

    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }

        return "DEFAULT_SNODE";
    }

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

    public int getSnodeHandleMqttThreadPoolQueueCapacity() {
        return snodeHandleMqttThreadPoolQueueCapacity;
    }

    public void setSnodeHandleMqttThreadPoolQueueCapacity(
            int snodeHandleMqttThreadPoolQueueCapacity) {
        this.snodeHandleMqttThreadPoolQueueCapacity = snodeHandleMqttThreadPoolQueueCapacity;
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

    public String getSnodeIP1() {
        return snodeIP1;
    }

    public void setSnodeIP1(String snodeIP1) {
        this.snodeIP1 = snodeIP1;
    }

    public String getSnodeIP2() {
        return snodeIP2;
    }

    public void setSnodeIP2(String snodeIP2) {
        this.snodeIP2 = snodeIP2;
    }

    public long getSnodeId() {
        return snodeId;
    }

    public void setSnodeId(long snodeId) {
        this.snodeId = snodeId;
    }

    public int getSnodeHandleMqttMessageMinPoolSize() {
        return snodeHandleMqttMessageMinPoolSize;
    }

    public void setSnodeHandleMqttMessageMinPoolSize(int snodeHandleMqttMessageMinPoolSize) {
        this.snodeHandleMqttMessageMinPoolSize = snodeHandleMqttMessageMinPoolSize;
    }

    public int getSnodeHandleMqttMessageMaxPoolSize() {
        return snodeHandleMqttMessageMaxPoolSize;
    }

    public void setSnodeHandleMqttMessageMaxPoolSize(int snodeHandleMqttMessageMaxPoolSize) {
        this.snodeHandleMqttMessageMaxPoolSize = snodeHandleMqttMessageMaxPoolSize;
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

    public String getSendMessageInterceptorPath() {
        return sendMessageInterceptorPath;
    }

    public String getConsumeMessageInterceptorPath() {
        return consumeMessageInterceptorPath;
    }

    public String getRemotingServerInterceptorPath() {
        return remotingServerInterceptorPath;
    }

    public int getSlowConsumerThreshold() {
        return slowConsumerThreshold;
    }

    public void setSlowConsumerThreshold(int slowConsumerThreshold) {
        this.slowConsumerThreshold = slowConsumerThreshold;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }
}
