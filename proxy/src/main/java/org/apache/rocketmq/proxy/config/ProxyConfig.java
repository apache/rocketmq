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

import org.apache.rocketmq.proxy.grpc.common.ProxyMode;

public class ProxyConfig {
    public final static String CONFIG_FILE_NAME = "rmq-proxy.json";

    /**
     * Configuration for proxy
     */
    private Integer healthCheckPort = 8000;
    private long waitAfterStopHealthCheckInSeconds = 40;

    /**
     * configuration for ThreadPoolMonitor
     */
    private boolean enablePrintJstack = true;
    private long printJstackInMillis = 60000;

    private String nameSrvAddr = "11.165.223.199:9876";
    private String nameSrvDomain = "";
    private String nameSrvDomainSubgroup = "";
    /**
     * gRPC
     */
    private String proxyMode = ProxyMode.CLUSTER.name();
    private Boolean startGrpcServer = true;
    private Integer grpcServerPort = 8081;
    private String grpcTlsKeyPath = ConfigurationManager.getProxyHome() + "/conf/tls/gRPC.key.pem";
    private String grpcTlsCertPath = ConfigurationManager.getProxyHome() + "/conf/tls/gRPC.chain.cert.pem";
    private int grpcBossLoopNum = 1;
    private int grpcWorkerLoopNum = Runtime.getRuntime().availableProcessors() * 2;
    private int grpcThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int grpcThreadPoolQueueCapacity = 100000;
    private String brokerConfigPath = ConfigurationManager.getProxyHome() + "/conf/broker.conf";
    /**
     * gRPC max message size
     * 130M = 4M * 32 messages + 2M attributes
     */
    private int grpcMaxInboundMessageSize = 130 * 1024 * 1024;

    private int channelExpiredInSeconds = 120;

    private int forwardConsumerNum = 2;
    private double forwardConsumerWorkerFactor = 0.2f;
    private int forwardProducerNum = 2;
    private double forwardProducerWorkerFactor = 0.2f;
    private int defaultForwardClientNum = 2;
    private double defaultForwardClientWorkerFactor = 0.2f;

    private int topicRouteCacheExpiredInSeconds = 20;
    private int topicRouteCacheExecutorThreadNum = 3;
    private int topicRouteCacheExecutorQueueCapacity = 1000;
    private int topicRouteCacheMaxNum = 20000;
    private int topicRouteThreadPoolNums = 36;
    private int topicRouteThreadPoolQueueCapacity = 50000;

    private int transactionHeartbeatThreadPoolNums = 20;
    private int transactionHeartbeatThreadPoolQueueCapacity = 200;
    private int transactionHeartbeatPeriodSecond = 20;
    private int transactionHeartbeatBatchNum = 100;

    private int longPollingReserveTimeInMillis = 10000;

    private int retryDelayLevelDelta = 3;
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    public Integer getHealthCheckPort() {
        return healthCheckPort;
    }

    public void setHealthCheckPort(Integer healthCheckPort) {
        this.healthCheckPort = healthCheckPort;
    }

    public long getWaitAfterStopHealthCheckInSeconds() {
        return waitAfterStopHealthCheckInSeconds;
    }

    public void setWaitAfterStopHealthCheckInSeconds(long waitAfterStopHealthCheckInSeconds) {
        this.waitAfterStopHealthCheckInSeconds = waitAfterStopHealthCheckInSeconds;
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

    public Boolean getStartGrpcServer() {
        return startGrpcServer;
    }

    public void setStartGrpcServer(Boolean startGrpcServer) {
        this.startGrpcServer = startGrpcServer;
    }

    public Integer getGrpcServerPort() {
        return grpcServerPort;
    }

    public void setGrpcServerPort(Integer grpcServerPort) {
        this.grpcServerPort = grpcServerPort;
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

    public int getForwardConsumerNum() {
        return forwardConsumerNum;
    }

    public void setForwardConsumerNum(int forwardConsumerNum) {
        this.forwardConsumerNum = forwardConsumerNum;
    }

    public double getForwardConsumerWorkerFactor() {
        return forwardConsumerWorkerFactor;
    }

    public void setForwardConsumerWorkerFactor(double forwardConsumerWorkerFactor) {
        this.forwardConsumerWorkerFactor = forwardConsumerWorkerFactor;
    }

    public int getForwardProducerNum() {
        return forwardProducerNum;
    }

    public void setForwardProducerNum(int forwardProducerNum) {
        this.forwardProducerNum = forwardProducerNum;
    }

    public double getForwardProducerWorkerFactor() {
        return forwardProducerWorkerFactor;
    }

    public void setForwardProducerWorkerFactor(double forwardProducerWorkerFactor) {
        this.forwardProducerWorkerFactor = forwardProducerWorkerFactor;
    }

    public int getDefaultForwardClientNum() {
        return defaultForwardClientNum;
    }

    public void setDefaultForwardClientNum(int defaultForwardClientNum) {
        this.defaultForwardClientNum = defaultForwardClientNum;
    }

    public double getDefaultForwardClientWorkerFactor() {
        return defaultForwardClientWorkerFactor;
    }

    public void setDefaultForwardClientWorkerFactor(double defaultForwardClientWorkerFactor) {
        this.defaultForwardClientWorkerFactor = defaultForwardClientWorkerFactor;
    }

    public int getTopicRouteCacheExpiredInSeconds() {
        return topicRouteCacheExpiredInSeconds;
    }

    public void setTopicRouteCacheExpiredInSeconds(int topicRouteCacheExpiredInSeconds) {
        this.topicRouteCacheExpiredInSeconds = topicRouteCacheExpiredInSeconds;
    }

    public int getTopicRouteCacheExecutorThreadNum() {
        return topicRouteCacheExecutorThreadNum;
    }

    public void setTopicRouteCacheExecutorThreadNum(int topicRouteCacheExecutorThreadNum) {
        this.topicRouteCacheExecutorThreadNum = topicRouteCacheExecutorThreadNum;
    }

    public int getTopicRouteCacheExecutorQueueCapacity() {
        return topicRouteCacheExecutorQueueCapacity;
    }

    public void setTopicRouteCacheExecutorQueueCapacity(int topicRouteCacheExecutorQueueCapacity) {
        this.topicRouteCacheExecutorQueueCapacity = topicRouteCacheExecutorQueueCapacity;
    }

    public int getTopicRouteCacheMaxNum() {
        return topicRouteCacheMaxNum;
    }

    public void setTopicRouteCacheMaxNum(int topicRouteCacheMaxNum) {
        this.topicRouteCacheMaxNum = topicRouteCacheMaxNum;
    }

    public int getTopicRouteThreadPoolNums() {
        return topicRouteThreadPoolNums;
    }

    public void setTopicRouteThreadPoolNums(int topicRouteThreadPoolNums) {
        this.topicRouteThreadPoolNums = topicRouteThreadPoolNums;
    }

    public int getTopicRouteThreadPoolQueueCapacity() {
        return topicRouteThreadPoolQueueCapacity;
    }

    public void setTopicRouteThreadPoolQueueCapacity(int topicRouteThreadPoolQueueCapacity) {
        this.topicRouteThreadPoolQueueCapacity = topicRouteThreadPoolQueueCapacity;
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

    public int getLongPollingReserveTimeInMillis() {
        return longPollingReserveTimeInMillis;
    }

    public void setLongPollingReserveTimeInMillis(int longPollingReserveTimeInMillis) {
        this.longPollingReserveTimeInMillis = longPollingReserveTimeInMillis;
    }

    public int getRetryDelayLevelDelta() {
        return retryDelayLevelDelta;
    }

    public void setRetryDelayLevelDelta(int retryDelayLevelDelta) {
        this.retryDelayLevelDelta = retryDelayLevelDelta;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }
}
