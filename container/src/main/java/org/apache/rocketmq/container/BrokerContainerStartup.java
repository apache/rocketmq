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
package org.apache.rocketmq.container;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class BrokerContainerStartup {
    private static final String BROKER_CONTAINER_CONFIG_OPTION = "c";
    private static final String BROKER_CONFIG_OPTION = "b";
    private static final String PRINT_PROPERTIES_OPTION = "p";
    private static final String PRINT_IMPORTANT_PROPERTIES_OPTION = "m";
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static Logger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();
    public static String rocketmqHome = null;

    public static void main(String[] args) {
        final BrokerContainer brokerContainer = startBrokerContainer(createBrokerContainer(args));
        createAndStartBrokers(brokerContainer);
    }

    public static BrokerController createBrokerController(String[] args) {
        final BrokerContainer brokerContainer = startBrokerContainer(createBrokerContainer(args));
        return createAndInitializeBroker(brokerContainer, configFile, properties);
    }

    public static List<BrokerController> createAndStartBrokers(BrokerContainer brokerContainer) {
        String[] configPaths = parseBrokerConfigPath();
        List<BrokerController> brokerControllerList = new ArrayList<>();

        if (configPaths != null && configPaths.length > 0) {
            SystemConfigFileHelper configFileHelper = new SystemConfigFileHelper();
            for (String configPath : configPaths) {
                System.out.printf("Start broker from config file path %s%n", configPath);
                configFileHelper.setFile(configPath);

                Properties brokerProperties = null;
                try {
                    brokerProperties = configFileHelper.loadConfig();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }

                final BrokerController brokerController = createAndInitializeBroker(brokerContainer, configPath, brokerProperties);
                if (brokerController != null) {
                    brokerControllerList.add(brokerController);
                    startBrokerController(brokerContainer, brokerController, brokerProperties);
                }
            }
        }

        return brokerControllerList;
    }

    public static String[] parseBrokerConfigPath() {
        String brokerConfigList = null;
        if (commandLine.hasOption(BROKER_CONFIG_OPTION)) {
            brokerConfigList = commandLine.getOptionValue(BROKER_CONFIG_OPTION);

        } else if (commandLine.hasOption(BROKER_CONTAINER_CONFIG_OPTION)) {
            String brokerContainerConfigPath = commandLine.getOptionValue(BROKER_CONTAINER_CONFIG_OPTION);
            if (brokerContainerConfigPath != null) {
                BrokerContainerConfig brokerContainerConfig = new BrokerContainerConfig();
                SystemConfigFileHelper configFileHelper = new SystemConfigFileHelper();
                configFileHelper.setFile(brokerContainerConfigPath);
                Properties brokerContainerProperties = null;
                try {
                    brokerContainerProperties = configFileHelper.loadConfig();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                if (brokerContainerProperties != null) {
                    MixAll.properties2Object(brokerContainerProperties, brokerContainerConfig);
                }
                brokerConfigList = brokerContainerConfig.getBrokerConfigPaths();
            }
        }

        if (brokerConfigList != null) {
            return brokerConfigList.split(":");
        }
        return null;
    }

    public static BrokerController createAndInitializeBroker(BrokerContainer brokerContainer,
        String filePath, Properties brokerProperties) {

        final BrokerConfig brokerConfig = new BrokerConfig();
        final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        if (brokerProperties != null) {
            properties2SystemEnv(brokerProperties);
            MixAll.properties2Object(brokerProperties, brokerConfig);
            MixAll.properties2Object(brokerProperties, messageStoreConfig);
        }

        messageStoreConfig.setHaListenPort(brokerConfig.getListenPort() + 1);

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("Slave's brokerId must be > 0%n");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }
        }

        if (messageStoreConfig.getTotalReplicas() < messageStoreConfig.getInSyncReplicas()
            || messageStoreConfig.getTotalReplicas() < messageStoreConfig.getMinInSyncReplicas()
            || messageStoreConfig.getInSyncReplicas() < messageStoreConfig.getMinInSyncReplicas()) {
            System.out.printf("invalid replicas number%n");
            System.exit(-3);
        }

        brokerConfig.setBrokerConfigPath(filePath);

        log = LoggerFactory.getLogger(brokerConfig.getIdentifier() + LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);

        try {
            BrokerController brokerController = brokerContainer.addBroker(brokerConfig, messageStoreConfig);
            if (brokerController != null) {
                brokerController.getConfiguration().registerConfig(brokerProperties);
                return brokerController;
            } else {
                System.out.printf("Add broker [%s-%s] failed.%n", brokerConfig.getBrokerName(), brokerConfig.getBrokerId());
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    public static BrokerContainer startBrokerContainer(BrokerContainer brokerContainer) {
        try {

            brokerContainer.start();

            String tip = "The broker container boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != brokerContainer.getBrokerContainerConfig().getNamesrvAddr()) {
                tip += " and name server is " + brokerContainer.getBrokerContainerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return brokerContainer;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static void startBrokerController(BrokerContainer brokerContainer,
        BrokerController brokerController, Properties brokerProperties) {
        try {
            for (BrokerBootHook hook : brokerContainer.getBrokerBootHookList()) {
                hook.executeBeforeStart(brokerController, brokerProperties);
            }

            brokerController.start();

            for (BrokerBootHook hook : brokerContainer.getBrokerBootHookList()) {
                hook.executeAfterStart(brokerController, brokerProperties);
            }

            String tip = String.format("Broker [%s-%s] boot success. serializeType=%s",
                brokerController.getBrokerConfig().getBrokerName(),
                brokerController.getBrokerConfig().getBrokerId(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            log.info(tip);
            System.out.printf("%s%n", tip);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void shutdown(final BrokerContainer controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    public static BrokerContainer createBrokerContainer(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new DefaultParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            final BrokerContainerConfig containerConfig = new BrokerContainerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            nettyServerConfig.setListenPort(10811);

            if (commandLine.hasOption(BROKER_CONTAINER_CONFIG_OPTION)) {
                String file = commandLine.getOptionValue(BROKER_CONTAINER_CONFIG_OPTION);
                if (file != null) {
                    CONFIG_FILE_HELPER.setFile(file);
                    configFile = file;
                    BrokerPathConfigHelper.setBrokerConfigPath(file);
                }
            }

            properties = CONFIG_FILE_HELPER.loadConfig();
            if (properties != null) {
                properties2SystemEnv(properties);
                MixAll.properties2Object(properties, containerConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), containerConfig);

            if (null == containerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }
            rocketmqHome = containerConfig.getRocketmqHome();

            String namesrvAddr = containerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        NetworkUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf(
                        "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            if (commandLine.hasOption(PRINT_PROPERTIES_OPTION)) {
                Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, containerConfig);
                MixAll.printObjectProperties(console, nettyServerConfig);
                MixAll.printObjectProperties(console, nettyClientConfig);
                System.exit(0);
            } else if (commandLine.hasOption(PRINT_IMPORTANT_PROPERTIES_OPTION)) {
                Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
                MixAll.printObjectProperties(console, containerConfig, true);
                MixAll.printObjectProperties(console, nettyServerConfig, true);
                MixAll.printObjectProperties(console, nettyClientConfig, true);
                System.exit(0);
            }

            log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
            MixAll.printObjectProperties(log, containerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);

            final BrokerContainer brokerContainer = new BrokerContainer(
                containerConfig,
                nettyServerConfig,
                nettyClientConfig);
            // remember all configs to prevent discard
            brokerContainer.getConfiguration().registerConfig(properties);

            boolean initResult = brokerContainer.initialize();
            if (!initResult) {
                brokerContainer.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long beginTime = System.currentTimeMillis();
                            brokerContainer.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return brokerContainer;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option(BROKER_CONTAINER_CONFIG_OPTION, "configFile", true, "Config file for shared broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(PRINT_PROPERTIES_OPTION, "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(PRINT_IMPORTANT_PROPERTIES_OPTION, "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(BROKER_CONFIG_OPTION, "brokerConfigFiles", true, "The path of broker config files, split by ':'");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            Properties properties = new Properties();

            try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                properties.load(in);
            }
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }

}
