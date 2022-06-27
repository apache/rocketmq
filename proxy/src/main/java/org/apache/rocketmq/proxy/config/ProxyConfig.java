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

package org.apache.rocketmq.proxy.config;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.ProxyMode;

public class ProxyConfig {
    public final static String CONFIG_FILE_NAME = "rmq-proxy.json";
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();

    private String rocketMQClusterName = "";

    /**
     * configuration for ThreadPoolMonitor
     */
    private boolean enablePrintJstack = true;
    private long printJstackInMillis = Duration.ofSeconds(60).toMillis();
    private long printThreadPoolStatusInMillis = Duration.ofSeconds(3).toMillis();

    private String nameSrvAddr = "";
    private String nameSrvDomain = "";
    private String nameSrvDomainSubgroup = "";
    /**
     * gRPC
     */
    private String proxyMode = ProxyMode.CLUSTER.name();
    private Integer grpcServerPort = 8081;
    private boolean grpcTlsTestModeEnable = true;
    private String grpcTlsKeyPath = ConfigurationManager.getProxyHome() + "/conf/tls/rocketmq.key";
    private String grpcTlsCertPath = ConfigurationManager.getProxyHome() + "/conf/tls/rocketmq.crt";
    private int grpcBossLoopNum = 1;
    private int grpcWorkerLoopNum = PROCESSOR_NUMBER * 2;
    private boolean enableGrpcEpoll = false;
    private int grpcThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int grpcThreadPoolQueueCapacity = 100000;
    private String brokerConfigPath = ConfigurationManager.getProxyHome() + "/conf/broker.conf";
    /**
     * gRPC max message size
     * 130M = 4M * 32 messages + 2M attributes
     */
    private int grpcMaxInboundMessageSize = 130 * 1024 * 1024;

    private int channelExpiredInSeconds = 60;
    private int contextExpiredInSeconds = 30;

    private int rocketmqMQClientNum = 6;

    private long grpcProxyRelayRequestTimeoutInSeconds = 5;
    private int grpcProducerThreadPoolNums = PROCESSOR_NUMBER;
    private int grpcProducerThreadQueueCapacity = 10000;
    private int grpcConsumerThreadPoolNums = PROCESSOR_NUMBER;
    private int grpcConsumerThreadQueueCapacity = 10000;
    private int grpcRouteThreadPoolNums = PROCESSOR_NUMBER;
    private int grpcRouteThreadQueueCapacity = 10000;
    private int grpcClientManagerThreadPoolNums = PROCESSOR_NUMBER;
    private int grpcClientManagerThreadQueueCapacity = 10000;
    private int grpcTransactionThreadPoolNums = PROCESSOR_NUMBER;
    private int grpcTransactionThreadQueueCapacity = 10000;

    private int producerProcessorThreadPoolNums = PROCESSOR_NUMBER;
    private int producerProcessorThreadPoolQueueCapacity = 10000;
    private int consumerProcessorThreadPoolNums = PROCESSOR_NUMBER;
    private int consumerProcessorThreadPoolQueueCapacity = 10000;

    private int topicRouteServiceCacheExpiredInSeconds = 20;
    private int topicRouteServiceCacheMaxNum = 20000;
    private int topicRouteServiceThreadPoolNums = PROCESSOR_NUMBER;
    private int topicRouteServiceThreadPoolQueueCapacity = 5000;

    private int topicConfigCacheExpiredInSeconds = 20;
    private int topicConfigCacheMaxNum = 20000;
    private int subscriptionGroupConfigCacheExpiredInSeconds = 20;
    private int subscriptionGroupConfigCacheMaxNum = 20000;
    private int metadataThreadPoolNums = 3;
    private int metadataThreadPoolQueueCapacity = 1000;

    private int transactionHeartbeatThreadPoolNums = 20;
    private int transactionHeartbeatThreadPoolQueueCapacity = 200;
    private int transactionHeartbeatPeriodSecond = 20;
    private int transactionHeartbeatBatchNum = 100;
    private long transactionDataExpireScanPeriodMillis = Duration.ofSeconds(10).toMillis();
    private long transactionDataMaxWaitClearMillis = Duration.ofSeconds(30).toMillis();
    private long defaultTransactionCheckImmunityTimeInMills = Duration.ofMillis(1).toMillis();

    private long longPollingReserveTimeInMillis = 100;

    private long invisibleTimeMillisWhenClear = 1000L;
    private boolean enableProxyAutoRenew = true;
    private long renewAheadTimeMillis = TimeUnit.SECONDS.toMillis(10);
    private long renewSliceTimeMillis = TimeUnit.SECONDS.toMillis(60);
    private long renewSchedulePeriodMillis = TimeUnit.SECONDS.toMillis(5);

    private boolean enableACL = false;

    private boolean enableTopicMessageTypeCheck = true;

    private int metricCollectorMode = MetricCollectorMode.OFF.getOrdinal();
    // Example address: 127.0.0.1:1234
    private String metricCollectorAddress = "";

    public String getRocketMQClusterName() {
        return rocketMQClusterName;
    }

    public void setRocketMQClusterName(String rocketMQClusterName) {
        this.rocketMQClusterName = rocketMQClusterName;
    }

    public boolean isEnablePrintJstack() {
        return enablePrintJstack;
    }

    public void setEnablePrintJstack(boolean enablePrintJstack) {
        this.enablePrintJstack = enablePrintJstack;
    }

    public long getPrintJstackInMillis() {
        return printJstackInMillis;
    }

    public void setPrintJstackInMillis(long printJstackInMillis) {
        this.printJstackInMillis = printJstackInMillis;
    }

    public long getPrintThreadPoolStatusInMillis() {
        return printThreadPoolStatusInMillis;
    }

    public void setPrintThreadPoolStatusInMillis(long printThreadPoolStatusInMillis) {
        this.printThreadPoolStatusInMillis = printThreadPoolStatusInMillis;
    }

    public String getNameSrvAddr() {
        return nameSrvAddr;
    }

    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }

    public String getNameSrvDomain() {
        return nameSrvDomain;
    }

    public void setNameSrvDomain(String nameSrvDomain) {
        this.nameSrvDomain = nameSrvDomain;
    }

    public String getNameSrvDomainSubgroup() {
        return nameSrvDomainSubgroup;
    }

    public void setNameSrvDomainSubgroup(String nameSrvDomainSubgroup) {
        this.nameSrvDomainSubgroup = nameSrvDomainSubgroup;
    }

    public String getProxyMode() {
        return proxyMode;
    }

    public void setProxyMode(String proxyMode) {
        this.proxyMode = proxyMode;
    }

    public Integer getGrpcServerPort() {
        return grpcServerPort;
    }

    public void setGrpcServerPort(Integer grpcServerPort) {
        this.grpcServerPort = grpcServerPort;
    }

    public boolean isGrpcTlsTestModeEnable() {
        return grpcTlsTestModeEnable;
    }

    public void setGrpcTlsTestModeEnable(boolean grpcTlsTestModeEnable) {
        this.grpcTlsTestModeEnable = grpcTlsTestModeEnable;
    }

    public String getGrpcTlsKeyPath() {
        return grpcTlsKeyPath;
    }

    public void setGrpcTlsKeyPath(String grpcTlsKeyPath) {
        this.grpcTlsKeyPath = grpcTlsKeyPath;
    }

    public String getGrpcTlsCertPath() {
        return grpcTlsCertPath;
    }

    public void setGrpcTlsCertPath(String grpcTlsCertPath) {
        this.grpcTlsCertPath = grpcTlsCertPath;
    }

    public int getGrpcBossLoopNum() {
        return grpcBossLoopNum;
    }

    public void setGrpcBossLoopNum(int grpcBossLoopNum) {
        this.grpcBossLoopNum = grpcBossLoopNum;
    }

    public int getGrpcWorkerLoopNum() {
        return grpcWorkerLoopNum;
    }

    public void setGrpcWorkerLoopNum(int grpcWorkerLoopNum) {
        this.grpcWorkerLoopNum = grpcWorkerLoopNum;
    }

    public boolean isEnableGrpcEpoll() {
        return enableGrpcEpoll;
    }

    public void setEnableGrpcEpoll(boolean enableGrpcEpoll) {
        this.enableGrpcEpoll = enableGrpcEpoll;
    }

    public int getGrpcThreadPoolNums() {
        return grpcThreadPoolNums;
    }

    public void setGrpcThreadPoolNums(int grpcThreadPoolNums) {
        this.grpcThreadPoolNums = grpcThreadPoolNums;
    }

    public int getGrpcThreadPoolQueueCapacity() {
        return grpcThreadPoolQueueCapacity;
    }

    public void setGrpcThreadPoolQueueCapacity(int grpcThreadPoolQueueCapacity) {
        this.grpcThreadPoolQueueCapacity = grpcThreadPoolQueueCapacity;
    }

    public String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public void setBrokerConfigPath(String brokerConfigPath) {
        this.brokerConfigPath = brokerConfigPath;
    }

    public int getGrpcMaxInboundMessageSize() {
        return grpcMaxInboundMessageSize;
    }

    public void setGrpcMaxInboundMessageSize(int grpcMaxInboundMessageSize) {
        this.grpcMaxInboundMessageSize = grpcMaxInboundMessageSize;
    }

    public int getChannelExpiredInSeconds() {
        return channelExpiredInSeconds;
    }

    public void setChannelExpiredInSeconds(int channelExpiredInSeconds) {
        this.channelExpiredInSeconds = channelExpiredInSeconds;
    }

    public int getContextExpiredInSeconds() {
        return contextExpiredInSeconds;
    }

    public void setContextExpiredInSeconds(int contextExpiredInSeconds) {
        this.contextExpiredInSeconds = contextExpiredInSeconds;
    }

    public int getRocketmqMQClientNum() {
        return rocketmqMQClientNum;
    }

    public void setRocketmqMQClientNum(int rocketmqMQClientNum) {
        this.rocketmqMQClientNum = rocketmqMQClientNum;
    }

    public long getGrpcProxyRelayRequestTimeoutInSeconds() {
        return grpcProxyRelayRequestTimeoutInSeconds;
    }

    public void setGrpcProxyRelayRequestTimeoutInSeconds(long grpcProxyRelayRequestTimeoutInSeconds) {
        this.grpcProxyRelayRequestTimeoutInSeconds = grpcProxyRelayRequestTimeoutInSeconds;
    }

    public int getGrpcProducerThreadPoolNums() {
        return grpcProducerThreadPoolNums;
    }

    public void setGrpcProducerThreadPoolNums(int grpcProducerThreadPoolNums) {
        this.grpcProducerThreadPoolNums = grpcProducerThreadPoolNums;
    }

    public int getGrpcProducerThreadQueueCapacity() {
        return grpcProducerThreadQueueCapacity;
    }

    public void setGrpcProducerThreadQueueCapacity(int grpcProducerThreadQueueCapacity) {
        this.grpcProducerThreadQueueCapacity = grpcProducerThreadQueueCapacity;
    }

    public int getGrpcConsumerThreadPoolNums() {
        return grpcConsumerThreadPoolNums;
    }

    public void setGrpcConsumerThreadPoolNums(int grpcConsumerThreadPoolNums) {
        this.grpcConsumerThreadPoolNums = grpcConsumerThreadPoolNums;
    }

    public int getGrpcConsumerThreadQueueCapacity() {
        return grpcConsumerThreadQueueCapacity;
    }

    public void setGrpcConsumerThreadQueueCapacity(int grpcConsumerThreadQueueCapacity) {
        this.grpcConsumerThreadQueueCapacity = grpcConsumerThreadQueueCapacity;
    }

    public int getGrpcRouteThreadPoolNums() {
        return grpcRouteThreadPoolNums;
    }

    public void setGrpcRouteThreadPoolNums(int grpcRouteThreadPoolNums) {
        this.grpcRouteThreadPoolNums = grpcRouteThreadPoolNums;
    }

    public int getGrpcRouteThreadQueueCapacity() {
        return grpcRouteThreadQueueCapacity;
    }

    public void setGrpcRouteThreadQueueCapacity(int grpcRouteThreadQueueCapacity) {
        this.grpcRouteThreadQueueCapacity = grpcRouteThreadQueueCapacity;
    }

    public int getGrpcClientManagerThreadPoolNums() {
        return grpcClientManagerThreadPoolNums;
    }

    public void setGrpcClientManagerThreadPoolNums(int grpcClientManagerThreadPoolNums) {
        this.grpcClientManagerThreadPoolNums = grpcClientManagerThreadPoolNums;
    }

    public int getGrpcClientManagerThreadQueueCapacity() {
        return grpcClientManagerThreadQueueCapacity;
    }

    public void setGrpcClientManagerThreadQueueCapacity(int grpcClientManagerThreadQueueCapacity) {
        this.grpcClientManagerThreadQueueCapacity = grpcClientManagerThreadQueueCapacity;
    }

    public int getGrpcTransactionThreadPoolNums() {
        return grpcTransactionThreadPoolNums;
    }

    public void setGrpcTransactionThreadPoolNums(int grpcTransactionThreadPoolNums) {
        this.grpcTransactionThreadPoolNums = grpcTransactionThreadPoolNums;
    }

    public int getGrpcTransactionThreadQueueCapacity() {
        return grpcTransactionThreadQueueCapacity;
    }

    public void setGrpcTransactionThreadQueueCapacity(int grpcTransactionThreadQueueCapacity) {
        this.grpcTransactionThreadQueueCapacity = grpcTransactionThreadQueueCapacity;
    }

    public int getProducerProcessorThreadPoolNums() {
        return producerProcessorThreadPoolNums;
    }

    public void setProducerProcessorThreadPoolNums(int producerProcessorThreadPoolNums) {
        this.producerProcessorThreadPoolNums = producerProcessorThreadPoolNums;
    }

    public int getProducerProcessorThreadPoolQueueCapacity() {
        return producerProcessorThreadPoolQueueCapacity;
    }

    public void setProducerProcessorThreadPoolQueueCapacity(int producerProcessorThreadPoolQueueCapacity) {
        this.producerProcessorThreadPoolQueueCapacity = producerProcessorThreadPoolQueueCapacity;
    }

    public int getConsumerProcessorThreadPoolNums() {
        return consumerProcessorThreadPoolNums;
    }

    public void setConsumerProcessorThreadPoolNums(int consumerProcessorThreadPoolNums) {
        this.consumerProcessorThreadPoolNums = consumerProcessorThreadPoolNums;
    }

    public int getConsumerProcessorThreadPoolQueueCapacity() {
        return consumerProcessorThreadPoolQueueCapacity;
    }

    public void setConsumerProcessorThreadPoolQueueCapacity(int consumerProcessorThreadPoolQueueCapacity) {
        this.consumerProcessorThreadPoolQueueCapacity = consumerProcessorThreadPoolQueueCapacity;
    }

    public int getTopicRouteServiceCacheExpiredInSeconds() {
        return topicRouteServiceCacheExpiredInSeconds;
    }

    public void setTopicRouteServiceCacheExpiredInSeconds(int topicRouteServiceCacheExpiredInSeconds) {
        this.topicRouteServiceCacheExpiredInSeconds = topicRouteServiceCacheExpiredInSeconds;
    }

    public int getTopicRouteServiceCacheMaxNum() {
        return topicRouteServiceCacheMaxNum;
    }

    public void setTopicRouteServiceCacheMaxNum(int topicRouteServiceCacheMaxNum) {
        this.topicRouteServiceCacheMaxNum = topicRouteServiceCacheMaxNum;
    }

    public int getTopicRouteServiceThreadPoolNums() {
        return topicRouteServiceThreadPoolNums;
    }

    public void setTopicRouteServiceThreadPoolNums(int topicRouteServiceThreadPoolNums) {
        this.topicRouteServiceThreadPoolNums = topicRouteServiceThreadPoolNums;
    }

    public int getTopicRouteServiceThreadPoolQueueCapacity() {
        return topicRouteServiceThreadPoolQueueCapacity;
    }

    public void setTopicRouteServiceThreadPoolQueueCapacity(int topicRouteServiceThreadPoolQueueCapacity) {
        this.topicRouteServiceThreadPoolQueueCapacity = topicRouteServiceThreadPoolQueueCapacity;
    }

    public int getTopicConfigCacheExpiredInSeconds() {
        return topicConfigCacheExpiredInSeconds;
    }

    public void setTopicConfigCacheExpiredInSeconds(int topicConfigCacheExpiredInSeconds) {
        this.topicConfigCacheExpiredInSeconds = topicConfigCacheExpiredInSeconds;
    }

    public int getTopicConfigCacheMaxNum() {
        return topicConfigCacheMaxNum;
    }

    public void setTopicConfigCacheMaxNum(int topicConfigCacheMaxNum) {
        this.topicConfigCacheMaxNum = topicConfigCacheMaxNum;
    }

    public int getSubscriptionGroupConfigCacheExpiredInSeconds() {
        return subscriptionGroupConfigCacheExpiredInSeconds;
    }

    public void setSubscriptionGroupConfigCacheExpiredInSeconds(int subscriptionGroupConfigCacheExpiredInSeconds) {
        this.subscriptionGroupConfigCacheExpiredInSeconds = subscriptionGroupConfigCacheExpiredInSeconds;
    }

    public int getSubscriptionGroupConfigCacheMaxNum() {
        return subscriptionGroupConfigCacheMaxNum;
    }

    public void setSubscriptionGroupConfigCacheMaxNum(int subscriptionGroupConfigCacheMaxNum) {
        this.subscriptionGroupConfigCacheMaxNum = subscriptionGroupConfigCacheMaxNum;
    }

    public int getMetadataThreadPoolNums() {
        return metadataThreadPoolNums;
    }

    public void setMetadataThreadPoolNums(int metadataThreadPoolNums) {
        this.metadataThreadPoolNums = metadataThreadPoolNums;
    }

    public int getMetadataThreadPoolQueueCapacity() {
        return metadataThreadPoolQueueCapacity;
    }

    public void setMetadataThreadPoolQueueCapacity(int metadataThreadPoolQueueCapacity) {
        this.metadataThreadPoolQueueCapacity = metadataThreadPoolQueueCapacity;
    }

    public int getTransactionHeartbeatThreadPoolNums() {
        return transactionHeartbeatThreadPoolNums;
    }

    public void setTransactionHeartbeatThreadPoolNums(int transactionHeartbeatThreadPoolNums) {
        this.transactionHeartbeatThreadPoolNums = transactionHeartbeatThreadPoolNums;
    }

    public int getTransactionHeartbeatThreadPoolQueueCapacity() {
        return transactionHeartbeatThreadPoolQueueCapacity;
    }

    public void setTransactionHeartbeatThreadPoolQueueCapacity(int transactionHeartbeatThreadPoolQueueCapacity) {
        this.transactionHeartbeatThreadPoolQueueCapacity = transactionHeartbeatThreadPoolQueueCapacity;
    }

    public int getTransactionHeartbeatPeriodSecond() {
        return transactionHeartbeatPeriodSecond;
    }

    public void setTransactionHeartbeatPeriodSecond(int transactionHeartbeatPeriodSecond) {
        this.transactionHeartbeatPeriodSecond = transactionHeartbeatPeriodSecond;
    }

    public int getTransactionHeartbeatBatchNum() {
        return transactionHeartbeatBatchNum;
    }

    public void setTransactionHeartbeatBatchNum(int transactionHeartbeatBatchNum) {
        this.transactionHeartbeatBatchNum = transactionHeartbeatBatchNum;
    }

    public long getTransactionDataExpireScanPeriodMillis() {
        return transactionDataExpireScanPeriodMillis;
    }

    public void setTransactionDataExpireScanPeriodMillis(long transactionDataExpireScanPeriodMillis) {
        this.transactionDataExpireScanPeriodMillis = transactionDataExpireScanPeriodMillis;
    }

    public long getTransactionDataMaxWaitClearMillis() {
        return transactionDataMaxWaitClearMillis;
    }

    public void setTransactionDataMaxWaitClearMillis(long transactionDataMaxWaitClearMillis) {
        this.transactionDataMaxWaitClearMillis = transactionDataMaxWaitClearMillis;
    }

    public long getDefaultTransactionCheckImmunityTimeInMills() {
        return defaultTransactionCheckImmunityTimeInMills;
    }

    public void setDefaultTransactionCheckImmunityTimeInMills(long defaultTransactionCheckImmunityTimeInMills) {
        this.defaultTransactionCheckImmunityTimeInMills = defaultTransactionCheckImmunityTimeInMills;
    }

    public long getLongPollingReserveTimeInMillis() {
        return longPollingReserveTimeInMillis;
    }

    public void setLongPollingReserveTimeInMillis(long longPollingReserveTimeInMillis) {
        this.longPollingReserveTimeInMillis = longPollingReserveTimeInMillis;
    }

    public boolean isEnableACL() {
        return enableACL;
    }

    public void setEnableACL(boolean enableACL) {
        this.enableACL = enableACL;
    }

    public boolean isEnableTopicMessageTypeCheck() {
        return enableTopicMessageTypeCheck;
    }

    public void setEnableTopicMessageTypeCheck(boolean enableTopicMessageTypeCheck) {
        this.enableTopicMessageTypeCheck = enableTopicMessageTypeCheck;
    }

    public long getInvisibleTimeMillisWhenClear() {
        return invisibleTimeMillisWhenClear;
    }

    public void setInvisibleTimeMillisWhenClear(long invisibleTimeMillisWhenClear) {
        this.invisibleTimeMillisWhenClear = invisibleTimeMillisWhenClear;
    }

    public boolean isEnableProxyAutoRenew() {
        return enableProxyAutoRenew;
    }

    public void setEnableProxyAutoRenew(boolean enableProxyAutoRenew) {
        this.enableProxyAutoRenew = enableProxyAutoRenew;
    }

    public long getRenewAheadTimeMillis() {
        return renewAheadTimeMillis;
    }

    public void setRenewAheadTimeMillis(long renewAheadTimeMillis) {
        this.renewAheadTimeMillis = renewAheadTimeMillis;
    }

    public long getRenewSliceTimeMillis() {
        return renewSliceTimeMillis;
    }

    public void setRenewSliceTimeMillis(long renewSliceTimeMillis) {
        this.renewSliceTimeMillis = renewSliceTimeMillis;
    }

    public long getRenewSchedulePeriodMillis() {
        return renewSchedulePeriodMillis;
    }

    public void setRenewSchedulePeriodMillis(long renewSchedulePeriodMillis) {
        this.renewSchedulePeriodMillis = renewSchedulePeriodMillis;
    }

    public int getMetricCollectorMode() {
        return metricCollectorMode;
    }

    public void setMetricCollectorMode(int metricCollectorMode) {
        this.metricCollectorMode = metricCollectorMode;
    }

    public String getMetricCollectorAddress() {
        return metricCollectorAddress;
    }

    public void setMetricCollectorAddress(String metricCollectorAddress) {
        this.metricCollectorAddress = metricCollectorAddress;
    }
}
