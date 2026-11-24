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
package org.apache.rocketmq.broker;

import java.util.Properties;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class ConfigContext {
    private String configFilePath;
    private Properties properties;

    private BrokerConfig brokerConfig;
    private NettyServerConfig nettyServerConfig;
    private NettyClientConfig nettyClientConfig;
    private MessageStoreConfig messageStoreConfig;
    private AuthConfig authConfig;

    private ConfigContext(Builder builder) {
        this.configFilePath = builder.configFilePath;
        this.properties = builder.properties;
        this.brokerConfig = builder.brokerConfig;
        this.nettyServerConfig = builder.nettyServerConfig;
        this.nettyClientConfig = builder.nettyClientConfig;
        this.messageStoreConfig = builder.messageStoreConfig;
        this.authConfig = builder.authConfig;
    }

    public String getConfigFilePath() {
        return configFilePath;
    }

    public Properties getProperties() {
        return properties;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public AuthConfig getAuthConfig() {
        return authConfig;
    }

    public static class Builder {
        private String configFilePath;
        private Properties properties;

        private BrokerConfig brokerConfig;
        private NettyServerConfig nettyServerConfig;
        private NettyClientConfig nettyClientConfig;
        private MessageStoreConfig messageStoreConfig;
        private AuthConfig authConfig;

        public Builder() {
        }

        public Builder configFilePath(String configFilePath) {
            this.configFilePath = configFilePath;
            return this;
        }

        public Builder properties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder brokerConfig(BrokerConfig brokerConfig) {
            this.brokerConfig = brokerConfig;
            return this;
        }

        public Builder nettyServerConfig(NettyServerConfig nettyServerConfig) {
            this.nettyServerConfig = nettyServerConfig;
            return this;
        }

        public Builder nettyClientConfig(NettyClientConfig nettyClientConfig) {
            this.nettyClientConfig = nettyClientConfig;
            return this;
        }

        public Builder messageStoreConfig(MessageStoreConfig messageStoreConfig) {
            this.messageStoreConfig = messageStoreConfig;
            return this;
        }

        public Builder authConfig(AuthConfig authConfig) {
            this.authConfig = authConfig;
            return this;
        }

        public ConfigContext build() {
            return new ConfigContext(this);
        }
    }
}
