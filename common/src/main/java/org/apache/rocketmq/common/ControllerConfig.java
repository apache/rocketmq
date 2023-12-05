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
package org.apache.rocketmq.common;

import java.io.File;
import java.util.Arrays;
import org.apache.rocketmq.common.metrics.MetricsExporterType;

public class ControllerConfig {
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    private String configStorePath = System.getProperty("user.home") + File.separator + "controller" + File.separator + "controller.properties";
    public static final String DLEDGER_CONTROLLER = "DLedger";
    public static final String JRAFT_CONTROLLER = "jRaft";

    private JraftConfig jraftConfig = new JraftConfig();

    private String controllerType = DLEDGER_CONTROLLER;
    /**
     * Interval of periodic scanning for non-active broker;
     * Unit: millisecond
     */
    private long scanNotActiveBrokerInterval = 5 * 1000;

    /**
     * Indicates the nums of thread to handle broker or operation requests, like REGISTER_BROKER.
     */
    private int controllerThreadPoolNums = 16;

    /**
     * Indicates the capacity of queue to hold client requests.
     */
    private int controllerRequestThreadPoolQueueCapacity = 50000;

    private String controllerDLegerGroup;
    private String controllerDLegerPeers;
    private String controllerDLegerSelfId;
    private int mappedFileSize = 1024 * 1024 * 1024;
    private String controllerStorePath = System.getProperty("user.home") + File.separator + controllerType + "Controller";

    /**
     * Whether the controller can elect a master which is not in the syncStateSet.
     */
    private boolean enableElectUncleanMaster = false;

    /**
     * Whether process read event
     */
    private boolean isProcessReadEvent = false;

    /**
     * Whether notify broker when its role changed
     */
    private volatile boolean notifyBrokerRoleChanged = true;
    /**
     * Interval of periodic scanning for non-active master in each broker-set;
     * Unit: millisecond
     */
    private long scanInactiveMasterInterval = 5 * 1000;

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

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getConfigStorePath() {
        return configStorePath;
    }

    public void setConfigStorePath(String configStorePath) {
        this.configStorePath = configStorePath;
    }

    public long getScanNotActiveBrokerInterval() {
        return scanNotActiveBrokerInterval;
    }

    public void setScanNotActiveBrokerInterval(long scanNotActiveBrokerInterval) {
        this.scanNotActiveBrokerInterval = scanNotActiveBrokerInterval;
    }

    public int getControllerThreadPoolNums() {
        return controllerThreadPoolNums;
    }

    public void setControllerThreadPoolNums(int controllerThreadPoolNums) {
        this.controllerThreadPoolNums = controllerThreadPoolNums;
    }

    public int getControllerRequestThreadPoolQueueCapacity() {
        return controllerRequestThreadPoolQueueCapacity;
    }

    public void setControllerRequestThreadPoolQueueCapacity(int controllerRequestThreadPoolQueueCapacity) {
        this.controllerRequestThreadPoolQueueCapacity = controllerRequestThreadPoolQueueCapacity;
    }

    public String getControllerDLegerGroup() {
        return controllerDLegerGroup;
    }

    public void setControllerDLegerGroup(String controllerDLegerGroup) {
        this.controllerDLegerGroup = controllerDLegerGroup;
    }

    public String getControllerDLegerPeers() {
        return controllerDLegerPeers;
    }

    public void setControllerDLegerPeers(String controllerDLegerPeers) {
        this.controllerDLegerPeers = controllerDLegerPeers;
    }

    public String getControllerDLegerSelfId() {
        return controllerDLegerSelfId;
    }

    public void setControllerDLegerSelfId(String controllerDLegerSelfId) {
        this.controllerDLegerSelfId = controllerDLegerSelfId;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public void setMappedFileSize(int mappedFileSize) {
        this.mappedFileSize = mappedFileSize;
    }

    public String getControllerStorePath() {
        return controllerStorePath;
    }

    public void setControllerStorePath(String controllerStorePath) {
        this.controllerStorePath = controllerStorePath;
    }

    public boolean isEnableElectUncleanMaster() {
        return enableElectUncleanMaster;
    }

    public void setEnableElectUncleanMaster(boolean enableElectUncleanMaster) {
        this.enableElectUncleanMaster = enableElectUncleanMaster;
    }

    public boolean isProcessReadEvent() {
        return isProcessReadEvent;
    }

    public void setProcessReadEvent(boolean processReadEvent) {
        isProcessReadEvent = processReadEvent;
    }

    public boolean isNotifyBrokerRoleChanged() {
        return notifyBrokerRoleChanged;
    }

    public void setNotifyBrokerRoleChanged(boolean notifyBrokerRoleChanged) {
        this.notifyBrokerRoleChanged = notifyBrokerRoleChanged;
    }

    public long getScanInactiveMasterInterval() {
        return scanInactiveMasterInterval;
    }

    public void setScanInactiveMasterInterval(long scanInactiveMasterInterval) {
        this.scanInactiveMasterInterval = scanInactiveMasterInterval;
    }

    public String getDLedgerAddress() {
        return Arrays.stream(this.controllerDLegerPeers.split(";"))
            .filter(x -> this.controllerDLegerSelfId.equals(x.split("-")[0]))
            .map(x -> x.split("-")[1]).findFirst().get();
    }

    public MetricsExporterType getMetricsExporterType() {
        return metricsExporterType;
    }

    public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
        this.metricsExporterType = metricsExporterType;
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

    public String getControllerType() {
        return controllerType;
    }

    public void setControllerType(String controllerType) {
        this.controllerType = controllerType;
    }

    public JraftConfig getJraftConfig() {
        return jraftConfig;
    }

    public void setJraftConfig(JraftConfig jraftConfig) {
        this.jraftConfig = jraftConfig;
    }
}
