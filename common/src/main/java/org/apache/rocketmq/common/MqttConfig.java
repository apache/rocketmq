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

import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;

public class MqttConfig {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private ServerConfig mqttServerConfig;

    private ClientConfig mqttClientConfig;

    private int handleMqttThreadPoolQueueCapacity = 10000;

    private int handleMqttMessageMinPoolSize = 10;

    private int handleMqttMessageMaxPoolSize = 20;

    private int pushMqttMessageMinPoolSize = 10;

    private int pushMqttMessageMaxPoolSize = 20;

    private int pushMqttMessageThreadPoolQueueCapacity = 10000;

    private int listenPort = 1883;
    /**
     * Acl feature switch
     */
    @ImportantField
    private boolean aclEnable = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }


    public boolean isAclEnable() {
        return aclEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }

    public ServerConfig getMqttServerConfig() {
        return mqttServerConfig;
    }

    public void setMqttServerConfig(ServerConfig mqttServerConfig) {
        this.mqttServerConfig = mqttServerConfig;
    }

    public ClientConfig getMqttClientConfig() {
        return mqttClientConfig;
    }

    public void setMqttClientConfig(ClientConfig mqttClientConfig) {
        this.mqttClientConfig = mqttClientConfig;
    }

    public int getHandleMqttThreadPoolQueueCapacity() {
        return handleMqttThreadPoolQueueCapacity;
    }

    public void setHandleMqttThreadPoolQueueCapacity(int handleMqttThreadPoolQueueCapacity) {
        this.handleMqttThreadPoolQueueCapacity = handleMqttThreadPoolQueueCapacity;
    }

    public int getHandleMqttMessageMinPoolSize() {
        return handleMqttMessageMinPoolSize;
    }

    public void setHandleMqttMessageMinPoolSize(int handleMqttMessageMinPoolSize) {
        this.handleMqttMessageMinPoolSize = handleMqttMessageMinPoolSize;
    }

    public int getHandleMqttMessageMaxPoolSize() {
        return handleMqttMessageMaxPoolSize;
    }

    public void setHandleMqttMessageMaxPoolSize(int handleMqttMessageMaxPoolSize) {
        this.handleMqttMessageMaxPoolSize = handleMqttMessageMaxPoolSize;
    }

    public int getPushMqttMessageMinPoolSize() {
        return pushMqttMessageMinPoolSize;
    }

    public void setPushMqttMessageMinPoolSize(int pushMqttMessageMinPoolSize) {
        this.pushMqttMessageMinPoolSize = pushMqttMessageMinPoolSize;
    }

    public int getPushMqttMessageMaxPoolSize() {
        return pushMqttMessageMaxPoolSize;
    }

    public void setPushMqttMessageMaxPoolSize(int pushMqttMessageMaxPoolSize) {
        this.pushMqttMessageMaxPoolSize = pushMqttMessageMaxPoolSize;
    }

    public int getPushMqttMessageThreadPoolQueueCapacity() {
        return pushMqttMessageThreadPoolQueueCapacity;
    }

    public void setPushMqttMessageThreadPoolQueueCapacity(int pushMqttMessageThreadPoolQueueCapacity) {
        this.pushMqttMessageThreadPoolQueueCapacity = pushMqttMessageThreadPoolQueueCapacity;
    }
}
