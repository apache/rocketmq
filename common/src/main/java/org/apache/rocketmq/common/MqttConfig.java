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

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class MqttConfig {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private int snodeHandleMqttThreadPoolQueueCapacity = 10000;

    private int snodeHandleMqttMessageMinPoolSize = 10;

    private int snodeHandleMqttMessageMaxPoolSize = 20;

    private long houseKeepingInterval = 10 * 1000;

    private int snodePushMqttMessageMinPoolSize = 10;

    private int snodePushMqttMessageMaxPoolSize = 20;

    private int snodePushMqttMessageThreadPoolQueueCapacity = 10000;

    private int listenPort = 1883;
    /**
     * Acl feature switch
     */
    @ImportantField
    private boolean aclEnable = false;

    public long getHouseKeepingInterval() {
        return houseKeepingInterval;
    }

    public void setHouseKeepingInterval(long houseKeepingInterval) {
        this.houseKeepingInterval = houseKeepingInterval;
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

    public int getSnodeHandleMqttThreadPoolQueueCapacity() {
        return snodeHandleMqttThreadPoolQueueCapacity;
    }

    public void setSnodeHandleMqttThreadPoolQueueCapacity(int snodeHandleMqttThreadPoolQueueCapacity) {
        this.snodeHandleMqttThreadPoolQueueCapacity = snodeHandleMqttThreadPoolQueueCapacity;
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

    public int getSnodePushMqttMessageMinPoolSize() {
        return snodePushMqttMessageMinPoolSize;
    }

    public void setSnodePushMqttMessageMinPoolSize(int snodePushMqttMessageMinPoolSize) {
        this.snodePushMqttMessageMinPoolSize = snodePushMqttMessageMinPoolSize;
    }

    public int getSnodePushMqttMessageMaxPoolSize() {
        return snodePushMqttMessageMaxPoolSize;
    }

    public void setSnodePushMqttMessageMaxPoolSize(int snodePushMqttMessageMaxPoolSize) {
        this.snodePushMqttMessageMaxPoolSize = snodePushMqttMessageMaxPoolSize;
    }

    public int getSnodePushMqttMessageThreadPoolQueueCapacity() {
        return snodePushMqttMessageThreadPoolQueueCapacity;
    }

    public void setSnodePushMqttMessageThreadPoolQueueCapacity(int snodePushMqttMessageThreadPoolQueueCapacity) {
        this.snodePushMqttMessageThreadPoolQueueCapacity = snodePushMqttMessageThreadPoolQueueCapacity;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }

}
