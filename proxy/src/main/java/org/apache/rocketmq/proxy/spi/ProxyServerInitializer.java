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

package org.apache.rocketmq.proxy.spi;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.CommandLineArgument;
import org.apache.rocketmq.proxy.ProxyMode;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProxyServerInitializer {

    private final CommandLineArgument commandLineArgument;

    private final MessagingProcessor messagingProcessor;
    private final ProxyMetricsManager proxyMetricsManager;
    private final BrokerController brokerController;
    private final StartAndShutdown brokerStartAndShutdownWrapper;
    private final List<StartAndShutdown> startAndShutdowns = new ArrayList<StartAndShutdown>();

    public ProxyServerInitializer(CommandLineArgument commandLineArgument) throws Exception {
        this.commandLineArgument = Objects.requireNonNull(commandLineArgument, "commandLineArgument is null");
        initializeConfiguration();
        validateConfiguration();
        initializeThreadPoolMonitor();
        // init services
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        if (ProxyMode.isClusterMode(proxyModeStr)) {
            this.brokerController = null;
            this.brokerStartAndShutdownWrapper = new StartAndShutdown() {
                @Override
                public void shutdown() throws Exception {

                }

                @Override
                public void start() throws Exception {

                }
            };
            this.messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
            this.proxyMetricsManager = ProxyMetricsManager.initClusterMode(ConfigurationManager.getProxyConfig());
        } else {
            this.brokerController = createBrokerController();
            ProxyMetricsManager.initLocalMode(brokerController.getBrokerMetricsManager(), ConfigurationManager.getProxyConfig());
            this.brokerStartAndShutdownWrapper = new StartAndShutdown() {
                @Override
                public void start() throws Exception {
                    brokerController.start();
                }

                @Override
                public void shutdown() throws Exception {
                    brokerController.shutdown();
                }
            };
            this.messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController);
            this.proxyMetricsManager = null;
        }
        //
        initializeStartAndShutdowns();
    }

    private void initializeConfiguration() throws Exception {
        if (StringUtils.isNotBlank(commandLineArgument.getProxyConfigPath())) {
            System.setProperty(Configuration.CONFIG_PATH_PROPERTY, commandLineArgument.getProxyConfigPath());
        }
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        if (StringUtils.isNotBlank(commandLineArgument.getNamesrvAddr())) {
            ConfigurationManager.getProxyConfig().setNamesrvAddr(commandLineArgument.getNamesrvAddr());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getBrokerConfigPath())) {
            ConfigurationManager.getProxyConfig().setBrokerConfigPath(commandLineArgument.getBrokerConfigPath());
        }
        if (StringUtils.isNotBlank(commandLineArgument.getProxyMode())) {
            ConfigurationManager.getProxyConfig().setProxyMode(commandLineArgument.getProxyMode());
        }
    }

    private void validateConfiguration() throws Exception {
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        if (!ProxyMode.isClusterMode(proxyModeStr) && !ProxyMode.isLocalMode(proxyModeStr)) {
            throw new IllegalArgumentException("try to start proxy server with wrong mode, use 'local' or 'cluster'");
        }
    }

    private void initializeThreadPoolMonitor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ThreadPoolMonitor.config(
                LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME),
                LoggerFactory.getLogger(LoggerName.PROXY_WATER_MARK_LOGGER_NAME),
                config.isEnablePrintJstack(), config.getPrintJstackInMillis(),
                config.getPrintThreadPoolStatusInMillis());
        ThreadPoolMonitor.init();
    }

    private BrokerController createBrokerController() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        List<String> brokerStartupArgList = Lists.newArrayList("-c", config.getBrokerConfigPath());
        if (StringUtils.isNotBlank(config.getNamesrvAddr())) {
            brokerStartupArgList.add("-n");
            brokerStartupArgList.add(config.getNamesrvAddr());
        }
        String[] brokerStartupArgs = brokerStartupArgList.toArray(new String[0]);
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    private void initializeStartAndShutdowns() throws Exception {
        this.startAndShutdowns.add(this.brokerStartAndShutdownWrapper);
        this.startAndShutdowns.add(this.messagingProcessor);
        if (this.proxyMetricsManager != null) {
            this.startAndShutdowns.add(this.proxyMetricsManager);
        }
    }

    public CommandLineArgument getCommandLineArgument() {
        return commandLineArgument;
    }

    public MessagingProcessor getMessagingProcessor() {
        return messagingProcessor;
    }

    public ProxyMetricsManager getProxyMetricsManager() {
        return proxyMetricsManager;
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public StartAndShutdown getBrokerStartAndShutdownWrapper() {
        return brokerStartAndShutdownWrapper;
    }

    public List<StartAndShutdown> getStartAndShutdowns() {
        return startAndShutdowns;
    }
}
