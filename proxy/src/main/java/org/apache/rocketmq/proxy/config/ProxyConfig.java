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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.ProxyMode;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class ProxyConfig implements ConfigFile {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    public final static String DEFAULT_CONFIG_FILE_NAME = "rmq-proxy.json";
    private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();
    private static final String DEFAULT_CLUSTER_NAME = "DefaultCluster";

    private static String localHostName;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }
    }

    private String rocketMQClusterName = DEFAULT_CLUSTER_NAME;
    private String proxyClusterName = DEFAULT_CLUSTER_NAME;
    private String proxyName = StringUtils.isEmpty(localHostName) ? "DEFAULT_PROXY" : localHostName;

    private String localServeAddr = "";

    private String heartbeatSyncerTopicClusterName = "";
    private int heartbeatSyncerThreadPoolNums = 4;
    private int heartbeatSyncerThreadPoolQueueCapacity = 100;

    private String heartbeatSyncerTopicName = "DefaultHeartBeatSyncerTopic";

    /**
     * configuration for ThreadPoolMonitor
     */
    private boolean enablePrintJstack = true;
    private long printJstackInMillis = Duration.ofSeconds(60).toMillis();
    private long printThreadPoolStatusInMillis = Duration.ofSeconds(3).toMillis();

    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    private String namesrvDomain = "";
    private String namesrvDomainSubgroup = "";
    /**
     * TLS
     */
    private boolean tlsTestModeEnable = true;
    private String tlsKeyPath = ConfigurationManager.getProxyHome() + "/conf/tls/rocketmq.key";
    private String tlsCertPath = ConfigurationManager.getProxyHome() + "/conf/tls/rocketmq.crt";
    /**
     * gRPC
     */
    private String proxyMode = ProxyMode.CLUSTER.name();
    private Integer grpcServerPort = 8081;
    private long grpcShutdownTimeSeconds = 30;
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
    /**
     * max message body size, 0 or negative number means no limit for proxy
     */
    private int maxMessageSize = 4 * 1024 * 1024;
    /**
     * max user property size, 0 or negative number means no limit for proxy
     */
    private int maxUserPropertySize = 16 * 1024;
    private int userPropertyMaxNum = 128;

    /**
     * max message group size, 0 or negative number means no limit for proxy
     */
    private int maxMessageGroupSize = 64;

    /**
     * When a message pops, the message is invisible by default
     */
    private long defaultInvisibleTimeMills = Duration.ofSeconds(60).toMillis();
    private long minInvisibleTimeMillsForRecv = Duration.ofSeconds(10).toMillis();
    private long maxInvisibleTimeMills = Duration.ofHours(12).toMillis();
    private long maxDelayTimeMills = Duration.ofDays(1).toMillis();
    private long maxTransactionRecoverySecond = Duration.ofHours(1).getSeconds();
    private boolean enableTopicMessageTypeCheck = true;

    private int grpcClientProducerMaxAttempts = 3;
    private long grpcClientProducerBackoffInitialMillis = 10;
    private long grpcClientProducerBackoffMaxMillis = 1000;
    private int grpcClientProducerBackoffMultiplier = 2;
    private long grpcClientConsumerMinLongPollingTimeoutMillis = Duration.ofSeconds(5).toMillis();
    private long grpcClientConsumerMaxLongPollingTimeoutMillis = Duration.ofSeconds(20).toMillis();
    private int grpcClientConsumerLongPollingBatchSize = 32;
    private long grpcClientIdleTimeMills = Duration.ofSeconds(120).toMillis();

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

    private boolean useEndpointPortFromRequest = false;

    private int topicRouteServiceCacheExpiredSeconds = 300;
    private int topicRouteServiceCacheRefreshSeconds = 20;
    private int topicRouteServiceCacheMaxNum = 20000;
    private int topicRouteServiceThreadPoolNums = PROCESSOR_NUMBER;
    private int topicRouteServiceThreadPoolQueueCapacity = 5000;
    private int topicConfigCacheExpiredSeconds = 300;
    private int topicConfigCacheRefreshSeconds = 20;
    private int topicConfigCacheMaxNum = 20000;
    private int subscriptionGroupConfigCacheExpiredSeconds = 300;
    private int subscriptionGroupConfigCacheRefreshSeconds = 20;
    private int subscriptionGroupConfigCacheMaxNum = 20000;
    private int userCacheExpiredSeconds = 300;
    private int userCacheRefreshSeconds = 20;
    private int userCacheMaxNum = 20000;
    private int aclCacheExpiredSeconds = 300;
    private int aclCacheRefreshSeconds = 20;
    private int aclCacheMaxNum = 20000;
    private int metadataThreadPoolNums = 3;
    private int metadataThreadPoolQueueCapacity = 100000;

    private int transactionHeartbeatThreadPoolNums = 20;
    private int transactionHeartbeatThreadPoolQueueCapacity = 200;
    private int transactionHeartbeatPeriodSecond = 20;
    private int transactionHeartbeatBatchNum = 100;
    private long transactionDataExpireScanPeriodMillis = Duration.ofSeconds(10).toMillis();
    private long transactionDataMaxWaitClearMillis = Duration.ofSeconds(30).toMillis();
    private long transactionDataExpireMillis = Duration.ofSeconds(30).toMillis();
    private int transactionDataMaxNum = 15;

    private long longPollingReserveTimeInMillis = 100;

    private long invisibleTimeMillisWhenClear = 1000L;
    private boolean enableProxyAutoRenew = true;
    private int maxRenewRetryTimes = 3;
    private int renewThreadPoolNums = 2;
    private int renewMaxThreadPoolNums = 4;
    private int renewThreadPoolQueueCapacity = 300;
    private long lockTimeoutMsInHandleGroup = TimeUnit.SECONDS.toMillis(3);
    private long renewAheadTimeMillis = TimeUnit.SECONDS.toMillis(10);
    private long renewMaxTimeMillis = TimeUnit.HOURS.toMillis(3);
    private long renewSchedulePeriodMillis = TimeUnit.SECONDS.toMillis(5);

    private boolean enableACL = false;

    private boolean enableAclRpcHookForClusterMode = false;

    private boolean useDelayLevel = false;
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    private transient Map<Integer /* level */, Long/* delay timeMillis */> delayLevelTable = new ConcurrentHashMap<>();

    private String metricCollectorMode = MetricCollectorMode.OFF.getModeString();
    // Example address: 127.0.0.1:1234
    private String metricCollectorAddress = "";

    private String regionId = "";

    private boolean traceOn = false;

    private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

    private String metricsGrpcExporterTarget = "";
    private String metricsGrpcExporterHeader = "";
    private long metricGrpcExporterTimeOutInMills = 3 * 1000;
    private long metricGrpcExporterIntervalInMills = 60 * 1000;
    private long metricLoggingExporterIntervalInMills = 10 * 1000;

    private int metricsPromExporterPort = 5557;
    private String metricsPromExporterHost = "";

    // Label pairs in CSV. Each label follows pattern of Key:Value. eg: instance_id:xxx,uid:xxx
    private String metricsLabel = "";

    private boolean metricsInDelta = false;

    private long channelExpiredTimeout = 1000 * 120;

    // remoting
    private boolean enableRemotingLocalProxyGrpc = true;
    private int localProxyConnectTimeoutMs = 3000;
    private String remotingAccessAddr = "";
    private int remotingListenPort = 8080;

    // related to proxy's send strategy in cluster mode.
    private boolean sendLatencyEnable = false;
    private boolean startDetectorEnable = false;
    private int detectTimeout = 200;
    private int detectInterval = 2 * 1000;

    private int remotingHeartbeatThreadPoolNums = 2 * PROCESSOR_NUMBER;
    private int remotingTopicRouteThreadPoolNums = 2 * PROCESSOR_NUMBER;
    private int remotingSendMessageThreadPoolNums = 4 * PROCESSOR_NUMBER;
    private int remotingPullMessageThreadPoolNums = 4 * PROCESSOR_NUMBER;
    private int remotingUpdateOffsetThreadPoolNums = 4 * PROCESSOR_NUMBER;
    private int remotingDefaultThreadPoolNums = 4 * PROCESSOR_NUMBER;

    private int remotingHeartbeatThreadPoolQueueCapacity = 50000;
    private int remotingTopicRouteThreadPoolQueueCapacity = 50000;
    private int remotingSendThreadPoolQueueCapacity = 10000;
    private int remotingPullThreadPoolQueueCapacity = 50000;
    private int remotingUpdateOffsetThreadPoolQueueCapacity = 10000;
    private int remotingDefaultThreadPoolQueueCapacity = 50000;

    private long remotingWaitTimeMillsInSendQueue = 3 * 1000;
    private long remotingWaitTimeMillsInPullQueue = 5 * 1000;
    private long remotingWaitTimeMillsInHeartbeatQueue = 31 * 1000;
    private long remotingWaitTimeMillsInUpdateOffsetQueue = 3 * 1000;
    private long remotingWaitTimeMillsInTopicRouteQueue = 3 * 1000;
    private long remotingWaitTimeMillsInDefaultQueue = 3 * 1000;

    private boolean enableBatchAck = false;

    @Override
    public void initData() {
        parseDelayLevel();
        if (StringUtils.isEmpty(localServeAddr)) {
            this.localServeAddr = NetworkUtil.getLocalAddress();
        }
        if (StringUtils.isBlank(localServeAddr)) {
            throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, "get local serve ip failed");
        }
        if (StringUtils.isBlank(remotingAccessAddr)) {
            this.remotingAccessAddr = this.localServeAddr;
        }
        if (StringUtils.isBlank(heartbeatSyncerTopicClusterName)) {
            this.heartbeatSyncerTopicClusterName = this.rocketMQClusterName;
        }
    }

    public int computeDelayLevel(long timeMillis) {
        long intervalMillis = timeMillis - System.currentTimeMillis();
        List<Map.Entry<Integer, Long>> sortedLevels = delayLevelTable.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getValue)).collect(Collectors.toList());
        for (Map.Entry<Integer, Long> entry : sortedLevels) {
            if (entry.getValue() > intervalMillis) {
                return entry.getKey();
            }
        }
        return sortedLevels.get(sortedLevels.size() - 1).getKey();
    }

    public void parseDelayLevel() {
        this.delayLevelTable = new ConcurrentHashMap<>();
        Map<String, Long> timeUnitTable = new HashMap<>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String levelString = this.getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parse delay level failed. messageDelayLevel:{}", messageDelayLevel, e);
        }
    }

    public String getRocketMQClusterName() {
        return rocketMQClusterName;
    }

    public void setRocketMQClusterName(String rocketMQClusterName) {
        this.rocketMQClusterName = rocketMQClusterName;
    }

    public String getProxyClusterName() {
        return proxyClusterName;
    }

    public void setProxyClusterName(String proxyClusterName) {
        this.proxyClusterName = proxyClusterName;
    }

    public String getProxyName() {
        return proxyName;
    }

    public void setProxyName(String proxyName) {
        this.proxyName = proxyName;
    }

    public String getLocalServeAddr() {
        return localServeAddr;
    }

    public void setLocalServeAddr(String localServeAddr) {
        this.localServeAddr = localServeAddr;
    }

    public String getHeartbeatSyncerTopicClusterName() {
        return heartbeatSyncerTopicClusterName;
    }

    public void setHeartbeatSyncerTopicClusterName(String heartbeatSyncerTopicClusterName) {
        this.heartbeatSyncerTopicClusterName = heartbeatSyncerTopicClusterName;
    }

    public int getHeartbeatSyncerThreadPoolNums() {
        return heartbeatSyncerThreadPoolNums;
    }

    public void setHeartbeatSyncerThreadPoolNums(int heartbeatSyncerThreadPoolNums) {
        this.heartbeatSyncerThreadPoolNums = heartbeatSyncerThreadPoolNums;
    }

    public int getHeartbeatSyncerThreadPoolQueueCapacity() {
        return heartbeatSyncerThreadPoolQueueCapacity;
    }

    public void setHeartbeatSyncerThreadPoolQueueCapacity(int heartbeatSyncerThreadPoolQueueCapacity) {
        this.heartbeatSyncerThreadPoolQueueCapacity = heartbeatSyncerThreadPoolQueueCapacity;
    }

    public String getHeartbeatSyncerTopicName() {
        return heartbeatSyncerTopicName;
    }

    public void setHeartbeatSyncerTopicName(String heartbeatSyncerTopicName) {
        this.heartbeatSyncerTopicName = heartbeatSyncerTopicName;
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

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getNamesrvDomain() {
        return namesrvDomain;
    }

    public void setNamesrvDomain(String namesrvDomain) {
        this.namesrvDomain = namesrvDomain;
    }

    public String getNamesrvDomainSubgroup() {
        return namesrvDomainSubgroup;
    }

    public void setNamesrvDomainSubgroup(String namesrvDomainSubgroup) {
        this.namesrvDomainSubgroup = namesrvDomainSubgroup;
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

    public long getGrpcShutdownTimeSeconds() {
        return grpcShutdownTimeSeconds;
    }

    public void setGrpcShutdownTimeSeconds(long grpcShutdownTimeSeconds) {
        this.grpcShutdownTimeSeconds = grpcShutdownTimeSeconds;
    }

    public boolean isUseEndpointPortFromRequest() {
        return useEndpointPortFromRequest;
    }

    public void setUseEndpointPortFromRequest(boolean useEndpointPortFromRequest) {
        this.useEndpointPortFromRequest = useEndpointPortFromRequest;
    }

    public boolean isTlsTestModeEnable() {
        return tlsTestModeEnable;
    }

    public void setTlsTestModeEnable(boolean tlsTestModeEnable) {
        this.tlsTestModeEnable = tlsTestModeEnable;
    }

    public String getTlsKeyPath() {
        return tlsKeyPath;
    }

    public void setTlsKeyPath(String tlsKeyPath) {
        this.tlsKeyPath = tlsKeyPath;
    }

    public String getTlsCertPath() {
        return tlsCertPath;
    }

    public void setTlsCertPath(String tlsCertPath) {
        this.tlsCertPath = tlsCertPath;
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

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getMaxUserPropertySize() {
        return maxUserPropertySize;
    }

    public void setMaxUserPropertySize(int maxUserPropertySize) {
        this.maxUserPropertySize = maxUserPropertySize;
    }

    public int getUserPropertyMaxNum() {
        return userPropertyMaxNum;
    }

    public void setUserPropertyMaxNum(int userPropertyMaxNum) {
        this.userPropertyMaxNum = userPropertyMaxNum;
    }

    public int getMaxMessageGroupSize() {
        return maxMessageGroupSize;
    }

    public void setMaxMessageGroupSize(int maxMessageGroupSize) {
        this.maxMessageGroupSize = maxMessageGroupSize;
    }

    public long getMinInvisibleTimeMillsForRecv() {
        return minInvisibleTimeMillsForRecv;
    }

    public void setMinInvisibleTimeMillsForRecv(long minInvisibleTimeMillsForRecv) {
        this.minInvisibleTimeMillsForRecv = minInvisibleTimeMillsForRecv;
    }

    public long getDefaultInvisibleTimeMills() {
        return defaultInvisibleTimeMills;
    }

    public void setDefaultInvisibleTimeMills(long defaultInvisibleTimeMills) {
        this.defaultInvisibleTimeMills = defaultInvisibleTimeMills;
    }

    public long getMaxInvisibleTimeMills() {
        return maxInvisibleTimeMills;
    }

    public void setMaxInvisibleTimeMills(long maxInvisibleTimeMills) {
        this.maxInvisibleTimeMills = maxInvisibleTimeMills;
    }

    public long getMaxDelayTimeMills() {
        return maxDelayTimeMills;
    }

    public void setMaxDelayTimeMills(long maxDelayTimeMills) {
        this.maxDelayTimeMills = maxDelayTimeMills;
    }

    public long getMaxTransactionRecoverySecond() {
        return maxTransactionRecoverySecond;
    }

    public void setMaxTransactionRecoverySecond(long maxTransactionRecoverySecond) {
        this.maxTransactionRecoverySecond = maxTransactionRecoverySecond;
    }

    public int getGrpcClientProducerMaxAttempts() {
        return grpcClientProducerMaxAttempts;
    }

    public void setGrpcClientProducerMaxAttempts(int grpcClientProducerMaxAttempts) {
        this.grpcClientProducerMaxAttempts = grpcClientProducerMaxAttempts;
    }

    public long getGrpcClientProducerBackoffInitialMillis() {
        return grpcClientProducerBackoffInitialMillis;
    }

    public void setGrpcClientProducerBackoffInitialMillis(long grpcClientProducerBackoffInitialMillis) {
        this.grpcClientProducerBackoffInitialMillis = grpcClientProducerBackoffInitialMillis;
    }

    public long getGrpcClientProducerBackoffMaxMillis() {
        return grpcClientProducerBackoffMaxMillis;
    }

    public void setGrpcClientProducerBackoffMaxMillis(long grpcClientProducerBackoffMaxMillis) {
        this.grpcClientProducerBackoffMaxMillis = grpcClientProducerBackoffMaxMillis;
    }

    public int getGrpcClientProducerBackoffMultiplier() {
        return grpcClientProducerBackoffMultiplier;
    }

    public void setGrpcClientProducerBackoffMultiplier(int grpcClientProducerBackoffMultiplier) {
        this.grpcClientProducerBackoffMultiplier = grpcClientProducerBackoffMultiplier;
    }

    public long getGrpcClientConsumerMinLongPollingTimeoutMillis() {
        return grpcClientConsumerMinLongPollingTimeoutMillis;
    }

    public void setGrpcClientConsumerMinLongPollingTimeoutMillis(long grpcClientConsumerMinLongPollingTimeoutMillis) {
        this.grpcClientConsumerMinLongPollingTimeoutMillis = grpcClientConsumerMinLongPollingTimeoutMillis;
    }

    public long getGrpcClientConsumerMaxLongPollingTimeoutMillis() {
        return grpcClientConsumerMaxLongPollingTimeoutMillis;
    }

    public void setGrpcClientConsumerMaxLongPollingTimeoutMillis(long grpcClientConsumerMaxLongPollingTimeoutMillis) {
        this.grpcClientConsumerMaxLongPollingTimeoutMillis = grpcClientConsumerMaxLongPollingTimeoutMillis;
    }

    public int getGrpcClientConsumerLongPollingBatchSize() {
        return grpcClientConsumerLongPollingBatchSize;
    }

    public void setGrpcClientConsumerLongPollingBatchSize(int grpcClientConsumerLongPollingBatchSize) {
        this.grpcClientConsumerLongPollingBatchSize = grpcClientConsumerLongPollingBatchSize;
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

    public int getTopicRouteServiceCacheExpiredSeconds() {
        return topicRouteServiceCacheExpiredSeconds;
    }

    public void setTopicRouteServiceCacheExpiredSeconds(int topicRouteServiceCacheExpiredSeconds) {
        this.topicRouteServiceCacheExpiredSeconds = topicRouteServiceCacheExpiredSeconds;
    }

    public int getTopicRouteServiceCacheRefreshSeconds() {
        return topicRouteServiceCacheRefreshSeconds;
    }

    public void setTopicRouteServiceCacheRefreshSeconds(int topicRouteServiceCacheRefreshSeconds) {
        this.topicRouteServiceCacheRefreshSeconds = topicRouteServiceCacheRefreshSeconds;
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

    public int getTopicConfigCacheRefreshSeconds() {
        return topicConfigCacheRefreshSeconds;
    }

    public void setTopicConfigCacheRefreshSeconds(int topicConfigCacheRefreshSeconds) {
        this.topicConfigCacheRefreshSeconds = topicConfigCacheRefreshSeconds;
    }

    public int getTopicConfigCacheExpiredSeconds() {
        return topicConfigCacheExpiredSeconds;
    }

    public void setTopicConfigCacheExpiredSeconds(int topicConfigCacheExpiredSeconds) {
        this.topicConfigCacheExpiredSeconds = topicConfigCacheExpiredSeconds;
    }

    public int getTopicConfigCacheMaxNum() {
        return topicConfigCacheMaxNum;
    }

    public void setTopicConfigCacheMaxNum(int topicConfigCacheMaxNum) {
        this.topicConfigCacheMaxNum = topicConfigCacheMaxNum;
    }

    public int getSubscriptionGroupConfigCacheRefreshSeconds() {
        return subscriptionGroupConfigCacheRefreshSeconds;
    }

    public void setSubscriptionGroupConfigCacheRefreshSeconds(int subscriptionGroupConfigCacheRefreshSeconds) {
        this.subscriptionGroupConfigCacheRefreshSeconds = subscriptionGroupConfigCacheRefreshSeconds;
    }

    public int getSubscriptionGroupConfigCacheExpiredSeconds() {
        return subscriptionGroupConfigCacheExpiredSeconds;
    }

    public void setSubscriptionGroupConfigCacheExpiredSeconds(int subscriptionGroupConfigCacheExpiredSeconds) {
        this.subscriptionGroupConfigCacheExpiredSeconds = subscriptionGroupConfigCacheExpiredSeconds;
    }

    public int getSubscriptionGroupConfigCacheMaxNum() {
        return subscriptionGroupConfigCacheMaxNum;
    }

    public void setSubscriptionGroupConfigCacheMaxNum(int subscriptionGroupConfigCacheMaxNum) {
        this.subscriptionGroupConfigCacheMaxNum = subscriptionGroupConfigCacheMaxNum;
    }

    public int getUserCacheExpiredSeconds() {
        return userCacheExpiredSeconds;
    }

    public void setUserCacheExpiredSeconds(int userCacheExpiredSeconds) {
        this.userCacheExpiredSeconds = userCacheExpiredSeconds;
    }

    public int getUserCacheRefreshSeconds() {
        return userCacheRefreshSeconds;
    }

    public void setUserCacheRefreshSeconds(int userCacheRefreshSeconds) {
        this.userCacheRefreshSeconds = userCacheRefreshSeconds;
    }

    public int getUserCacheMaxNum() {
        return userCacheMaxNum;
    }

    public void setUserCacheMaxNum(int userCacheMaxNum) {
        this.userCacheMaxNum = userCacheMaxNum;
    }

    public int getAclCacheExpiredSeconds() {
        return aclCacheExpiredSeconds;
    }

    public void setAclCacheExpiredSeconds(int aclCacheExpiredSeconds) {
        this.aclCacheExpiredSeconds = aclCacheExpiredSeconds;
    }

    public int getAclCacheRefreshSeconds() {
        return aclCacheRefreshSeconds;
    }

    public void setAclCacheRefreshSeconds(int aclCacheRefreshSeconds) {
        this.aclCacheRefreshSeconds = aclCacheRefreshSeconds;
    }

    public int getAclCacheMaxNum() {
        return aclCacheMaxNum;
    }

    public void setAclCacheMaxNum(int aclCacheMaxNum) {
        this.aclCacheMaxNum = aclCacheMaxNum;
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

    public long getTransactionDataExpireMillis() {
        return transactionDataExpireMillis;
    }

    public void setTransactionDataExpireMillis(long transactionDataExpireMillis) {
        this.transactionDataExpireMillis = transactionDataExpireMillis;
    }

    public int getTransactionDataMaxNum() {
        return transactionDataMaxNum;
    }

    public void setTransactionDataMaxNum(int transactionDataMaxNum) {
        this.transactionDataMaxNum = transactionDataMaxNum;
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

    public boolean isEnableAclRpcHookForClusterMode() {
        return enableAclRpcHookForClusterMode;
    }

    public void setEnableAclRpcHookForClusterMode(boolean enableAclRpcHookForClusterMode) {
        this.enableAclRpcHookForClusterMode = enableAclRpcHookForClusterMode;
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

    public int getMaxRenewRetryTimes() {
        return maxRenewRetryTimes;
    }

    public void setMaxRenewRetryTimes(int maxRenewRetryTimes) {
        this.maxRenewRetryTimes = maxRenewRetryTimes;
    }

    public int getRenewThreadPoolNums() {
        return renewThreadPoolNums;
    }

    public void setRenewThreadPoolNums(int renewThreadPoolNums) {
        this.renewThreadPoolNums = renewThreadPoolNums;
    }

    public int getRenewMaxThreadPoolNums() {
        return renewMaxThreadPoolNums;
    }

    public void setRenewMaxThreadPoolNums(int renewMaxThreadPoolNums) {
        this.renewMaxThreadPoolNums = renewMaxThreadPoolNums;
    }

    public int getRenewThreadPoolQueueCapacity() {
        return renewThreadPoolQueueCapacity;
    }

    public void setRenewThreadPoolQueueCapacity(int renewThreadPoolQueueCapacity) {
        this.renewThreadPoolQueueCapacity = renewThreadPoolQueueCapacity;
    }

    public long getLockTimeoutMsInHandleGroup() {
        return lockTimeoutMsInHandleGroup;
    }

    public void setLockTimeoutMsInHandleGroup(long lockTimeoutMsInHandleGroup) {
        this.lockTimeoutMsInHandleGroup = lockTimeoutMsInHandleGroup;
    }

    public long getRenewAheadTimeMillis() {
        return renewAheadTimeMillis;
    }

    public void setRenewAheadTimeMillis(long renewAheadTimeMillis) {
        this.renewAheadTimeMillis = renewAheadTimeMillis;
    }

    public long getRenewMaxTimeMillis() {
        return renewMaxTimeMillis;
    }

    public void setRenewMaxTimeMillis(long renewMaxTimeMillis) {
        this.renewMaxTimeMillis = renewMaxTimeMillis;
    }

    public long getRenewSchedulePeriodMillis() {
        return renewSchedulePeriodMillis;
    }

    public void setRenewSchedulePeriodMillis(long renewSchedulePeriodMillis) {
        this.renewSchedulePeriodMillis = renewSchedulePeriodMillis;
    }

    public String getMetricCollectorMode() {
        return metricCollectorMode;
    }

    public void setMetricCollectorMode(String metricCollectorMode) {
        this.metricCollectorMode = metricCollectorMode;
    }

    public String getMetricCollectorAddress() {
        return metricCollectorAddress;
    }

    public void setMetricCollectorAddress(String metricCollectorAddress) {
        this.metricCollectorAddress = metricCollectorAddress;
    }

    public boolean isUseDelayLevel() {
        return useDelayLevel;
    }

    public void setUseDelayLevel(boolean useDelayLevel) {
        this.useDelayLevel = useDelayLevel;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public Map<Integer, Long> getDelayLevelTable() {
        return delayLevelTable;
    }

    public long getGrpcClientIdleTimeMills() {
        return grpcClientIdleTimeMills;
    }

    public void setGrpcClientIdleTimeMills(final long grpcClientIdleTimeMills) {
        this.grpcClientIdleTimeMills = grpcClientIdleTimeMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(boolean traceOn) {
        this.traceOn = traceOn;
    }

    public String getRemotingAccessAddr() {
        return remotingAccessAddr;
    }

    public void setRemotingAccessAddr(String remotingAccessAddr) {
        this.remotingAccessAddr = remotingAccessAddr;
    }

    public MetricsExporterType getMetricsExporterType() {
        return metricsExporterType;
    }

    public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
        this.metricsExporterType = metricsExporterType;
    }

    public void setMetricsExporterType(int metricsExporterType) {
        this.metricsExporterType = MetricsExporterType.valueOf(metricsExporterType);
    }

    public void setMetricsExporterType(String metricsExporterType) {
        this.metricsExporterType = MetricsExporterType.valueOf(metricsExporterType);
    }

    public String getMetricsGrpcExporterTarget() {
        return metricsGrpcExporterTarget;
    }

    public void setMetricsGrpcExporterTarget(String metricsGrpcExporterTarget) {
        this.metricsGrpcExporterTarget = metricsGrpcExporterTarget;
    }

    public String getMetricsGrpcExporterHeader() {
        return metricsGrpcExporterHeader;
    }

    public void setMetricsGrpcExporterHeader(String metricsGrpcExporterHeader) {
        this.metricsGrpcExporterHeader = metricsGrpcExporterHeader;
    }

    public long getMetricGrpcExporterTimeOutInMills() {
        return metricGrpcExporterTimeOutInMills;
    }

    public void setMetricGrpcExporterTimeOutInMills(long metricGrpcExporterTimeOutInMills) {
        this.metricGrpcExporterTimeOutInMills = metricGrpcExporterTimeOutInMills;
    }

    public long getMetricGrpcExporterIntervalInMills() {
        return metricGrpcExporterIntervalInMills;
    }

    public void setMetricGrpcExporterIntervalInMills(long metricGrpcExporterIntervalInMills) {
        this.metricGrpcExporterIntervalInMills = metricGrpcExporterIntervalInMills;
    }

    public long getMetricLoggingExporterIntervalInMills() {
        return metricLoggingExporterIntervalInMills;
    }

    public void setMetricLoggingExporterIntervalInMills(long metricLoggingExporterIntervalInMills) {
        this.metricLoggingExporterIntervalInMills = metricLoggingExporterIntervalInMills;
    }

    public int getMetricsPromExporterPort() {
        return metricsPromExporterPort;
    }

    public void setMetricsPromExporterPort(int metricsPromExporterPort) {
        this.metricsPromExporterPort = metricsPromExporterPort;
    }

    public String getMetricsPromExporterHost() {
        return metricsPromExporterHost;
    }

    public void setMetricsPromExporterHost(String metricsPromExporterHost) {
        this.metricsPromExporterHost = metricsPromExporterHost;
    }

    public String getMetricsLabel() {
        return metricsLabel;
    }

    public void setMetricsLabel(String metricsLabel) {
        this.metricsLabel = metricsLabel;
    }

    public boolean isMetricsInDelta() {
        return metricsInDelta;
    }

    public void setMetricsInDelta(boolean metricsInDelta) {
        this.metricsInDelta = metricsInDelta;
    }

    public long getChannelExpiredTimeout() {
        return channelExpiredTimeout;
    }

    public boolean isEnableRemotingLocalProxyGrpc() {
        return enableRemotingLocalProxyGrpc;
    }

    public void setChannelExpiredTimeout(long channelExpiredTimeout) {
        this.channelExpiredTimeout = channelExpiredTimeout;
    }

    public void setEnableRemotingLocalProxyGrpc(boolean enableRemotingLocalProxyGrpc) {
        this.enableRemotingLocalProxyGrpc = enableRemotingLocalProxyGrpc;
    }

    public int getLocalProxyConnectTimeoutMs() {
        return localProxyConnectTimeoutMs;
    }

    public void setLocalProxyConnectTimeoutMs(int localProxyConnectTimeoutMs) {
        this.localProxyConnectTimeoutMs = localProxyConnectTimeoutMs;
    }

    public int getRemotingListenPort() {
        return remotingListenPort;
    }

    public void setRemotingListenPort(int remotingListenPort) {
        this.remotingListenPort = remotingListenPort;
    }

    public int getRemotingHeartbeatThreadPoolNums() {
        return remotingHeartbeatThreadPoolNums;
    }

    public void setRemotingHeartbeatThreadPoolNums(int remotingHeartbeatThreadPoolNums) {
        this.remotingHeartbeatThreadPoolNums = remotingHeartbeatThreadPoolNums;
    }

    public int getRemotingTopicRouteThreadPoolNums() {
        return remotingTopicRouteThreadPoolNums;
    }

    public void setRemotingTopicRouteThreadPoolNums(int remotingTopicRouteThreadPoolNums) {
        this.remotingTopicRouteThreadPoolNums = remotingTopicRouteThreadPoolNums;
    }

    public int getRemotingSendMessageThreadPoolNums() {
        return remotingSendMessageThreadPoolNums;
    }

    public void setRemotingSendMessageThreadPoolNums(int remotingSendMessageThreadPoolNums) {
        this.remotingSendMessageThreadPoolNums = remotingSendMessageThreadPoolNums;
    }

    public int getRemotingPullMessageThreadPoolNums() {
        return remotingPullMessageThreadPoolNums;
    }

    public void setRemotingPullMessageThreadPoolNums(int remotingPullMessageThreadPoolNums) {
        this.remotingPullMessageThreadPoolNums = remotingPullMessageThreadPoolNums;
    }

    public int getRemotingUpdateOffsetThreadPoolNums() {
        return remotingUpdateOffsetThreadPoolNums;
    }

    public void setRemotingUpdateOffsetThreadPoolNums(int remotingUpdateOffsetThreadPoolNums) {
        this.remotingUpdateOffsetThreadPoolNums = remotingUpdateOffsetThreadPoolNums;
    }

    public int getRemotingDefaultThreadPoolNums() {
        return remotingDefaultThreadPoolNums;
    }

    public void setRemotingDefaultThreadPoolNums(int remotingDefaultThreadPoolNums) {
        this.remotingDefaultThreadPoolNums = remotingDefaultThreadPoolNums;
    }

    public int getRemotingHeartbeatThreadPoolQueueCapacity() {
        return remotingHeartbeatThreadPoolQueueCapacity;
    }

    public void setRemotingHeartbeatThreadPoolQueueCapacity(int remotingHeartbeatThreadPoolQueueCapacity) {
        this.remotingHeartbeatThreadPoolQueueCapacity = remotingHeartbeatThreadPoolQueueCapacity;
    }

    public int getRemotingTopicRouteThreadPoolQueueCapacity() {
        return remotingTopicRouteThreadPoolQueueCapacity;
    }

    public void setRemotingTopicRouteThreadPoolQueueCapacity(int remotingTopicRouteThreadPoolQueueCapacity) {
        this.remotingTopicRouteThreadPoolQueueCapacity = remotingTopicRouteThreadPoolQueueCapacity;
    }

    public int getRemotingSendThreadPoolQueueCapacity() {
        return remotingSendThreadPoolQueueCapacity;
    }

    public void setRemotingSendThreadPoolQueueCapacity(int remotingSendThreadPoolQueueCapacity) {
        this.remotingSendThreadPoolQueueCapacity = remotingSendThreadPoolQueueCapacity;
    }

    public int getRemotingPullThreadPoolQueueCapacity() {
        return remotingPullThreadPoolQueueCapacity;
    }

    public void setRemotingPullThreadPoolQueueCapacity(int remotingPullThreadPoolQueueCapacity) {
        this.remotingPullThreadPoolQueueCapacity = remotingPullThreadPoolQueueCapacity;
    }

    public int getRemotingUpdateOffsetThreadPoolQueueCapacity() {
        return remotingUpdateOffsetThreadPoolQueueCapacity;
    }

    public void setRemotingUpdateOffsetThreadPoolQueueCapacity(int remotingUpdateOffsetThreadPoolQueueCapacity) {
        this.remotingUpdateOffsetThreadPoolQueueCapacity = remotingUpdateOffsetThreadPoolQueueCapacity;
    }

    public int getRemotingDefaultThreadPoolQueueCapacity() {
        return remotingDefaultThreadPoolQueueCapacity;
    }

    public void setRemotingDefaultThreadPoolQueueCapacity(int remotingDefaultThreadPoolQueueCapacity) {
        this.remotingDefaultThreadPoolQueueCapacity = remotingDefaultThreadPoolQueueCapacity;
    }

    public long getRemotingWaitTimeMillsInSendQueue() {
        return remotingWaitTimeMillsInSendQueue;
    }

    public void setRemotingWaitTimeMillsInSendQueue(long remotingWaitTimeMillsInSendQueue) {
        this.remotingWaitTimeMillsInSendQueue = remotingWaitTimeMillsInSendQueue;
    }

    public long getRemotingWaitTimeMillsInPullQueue() {
        return remotingWaitTimeMillsInPullQueue;
    }

    public void setRemotingWaitTimeMillsInPullQueue(long remotingWaitTimeMillsInPullQueue) {
        this.remotingWaitTimeMillsInPullQueue = remotingWaitTimeMillsInPullQueue;
    }

    public long getRemotingWaitTimeMillsInHeartbeatQueue() {
        return remotingWaitTimeMillsInHeartbeatQueue;
    }

    public void setRemotingWaitTimeMillsInHeartbeatQueue(long remotingWaitTimeMillsInHeartbeatQueue) {
        this.remotingWaitTimeMillsInHeartbeatQueue = remotingWaitTimeMillsInHeartbeatQueue;
    }

    public long getRemotingWaitTimeMillsInUpdateOffsetQueue() {
        return remotingWaitTimeMillsInUpdateOffsetQueue;
    }

    public void setRemotingWaitTimeMillsInUpdateOffsetQueue(long remotingWaitTimeMillsInUpdateOffsetQueue) {
        this.remotingWaitTimeMillsInUpdateOffsetQueue = remotingWaitTimeMillsInUpdateOffsetQueue;
    }

    public long getRemotingWaitTimeMillsInTopicRouteQueue() {
        return remotingWaitTimeMillsInTopicRouteQueue;
    }

    public void setRemotingWaitTimeMillsInTopicRouteQueue(long remotingWaitTimeMillsInTopicRouteQueue) {
        this.remotingWaitTimeMillsInTopicRouteQueue = remotingWaitTimeMillsInTopicRouteQueue;
    }

    public long getRemotingWaitTimeMillsInDefaultQueue() {
        return remotingWaitTimeMillsInDefaultQueue;
    }

    public void setRemotingWaitTimeMillsInDefaultQueue(long remotingWaitTimeMillsInDefaultQueue) {
        this.remotingWaitTimeMillsInDefaultQueue = remotingWaitTimeMillsInDefaultQueue;
    }

    public boolean isSendLatencyEnable() {
        return sendLatencyEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    public void setSendLatencyEnable(boolean sendLatencyEnable) {
        this.sendLatencyEnable = sendLatencyEnable;
    }

    public boolean getStartDetectorEnable() {
        return this.startDetectorEnable;
    }

    public boolean getSendLatencyEnable() {
        return this.sendLatencyEnable;
    }

    public int getDetectTimeout() {
        return detectTimeout;
    }

    public void setDetectTimeout(int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    public int getDetectInterval() {
        return detectInterval;
    }

    public void setDetectInterval(int detectInterval) {
        this.detectInterval = detectInterval;
    }

    public boolean isEnableBatchAck() {
        return enableBatchAck;
    }

    public void setEnableBatchAck(boolean enableBatchAck) {
        this.enableBatchAck = enableBatchAck;
    }
}
