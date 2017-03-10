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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *     This class is to read and parse command line arguments then start broker servers. It's the entry class of the
 *     broker servers.
 * </p>
 *
 * <p>
 *     <strong>Thread Safety:</strong> This class is the bootstrap for broker servers, no thread-safety is required.
 * </p>
 */
public class BrokerStartup {

    /**
     * {@link Properties} instance to store key-value pairs of specified broker configuration file.
     */
    public static Properties properties = null;

    /**
     * Command line argument parser.
     */
    public static CommandLine commandLine = null;

    /**
     * Broker configuration file.
     */
    public static String configFile = null;

    /**
     * Logger instance.
     */
    public static Logger log;

    /**
     * Main method, all things start here.
     *
     * @param args command line argument.
     */
    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    /**
     * Starts the {@link BrokerController} instance.
     *
     * @param controller BrokerController instance.
     * @return The same broker controller instance if successful; null otherwise.
     */
    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();
            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * This method creates {@link BrokerController} instance using command line arguments.
     *
     * @param args Command line arguments.
     * @return Instance of {@link BrokerController}.
     */
    public static BrokerController createBrokerController(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // Default send buffer size to 128KB
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        // Default receive buffer size to 128KB
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try {
            //PackageConflictDetect.detectFastjson();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options),
                new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
            }

            final BrokerConfig brokerConfig = new BrokerConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            nettyServerConfig.setListenPort(10911); // Set default broker listen port
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {

                // Use a conservative value for slave brokers so that consumer groups won't frequently change targeting
                // brokers when they are consuming messages at the boundary between being probably resident in physical
                // memory and swapped out.
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            // FIXME: log should not be null
            if (commandLine.hasOption('p')) { // print all configurables.
                MixAll.printObjectProperties(null, brokerConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                MixAll.printObjectProperties(null, nettyClientConfig);
                MixAll.printObjectProperties(null, messageStoreConfig);
                System.exit(0);
            } else if (commandLine.hasOption('m')) { // print all important configurations
                MixAll.printObjectProperties(null, brokerConfig, true);
                MixAll.printObjectProperties(null, nettyServerConfig, true);
                MixAll.printObjectProperties(null, nettyClientConfig, true);
                MixAll.printObjectProperties(null, messageStoreConfig, true);
                System.exit(0);
            }

            // Load and apply configurations through file.
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null && !file.trim().isEmpty()) {
                    configFile = file.trim();
                    File f = new File(configFile);
                    if (f.exists() && f.canRead()) {
                        try (InputStream in = new BufferedInputStream(new FileInputStream(f))) {
                            properties = new Properties();
                            properties.load(in);

                            properties2SystemEnv(properties);
                            MixAll.properties2Object(properties, brokerConfig);
                            MixAll.properties2Object(properties, nettyServerConfig);
                            MixAll.properties2Object(properties, nettyClientConfig);
                            MixAll.properties2Object(properties, messageStoreConfig);

                            BrokerPathConfigHelper.setBrokerConfigPath(file);
                        }
                    } else {
                        System.out.printf("%s does not exist, abort.", file);
                        System.exit(-2);
                    }
                }
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            // Check ROCKETMQ_HOME environment variable.
            if (null == brokerConfig.getRocketmqHome()) {
                System.out.printf("Please set the %s variable in your environment to match the location of the " +
                    "RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
                System.exit(-2);
            }

            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    for (String addr : addrArray) {
                        RemotingUtil.string2SocketAddress(addr);
                    }
                } catch (Exception e) {
                    System.out.printf("The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                        namesrvAddr);
                    System.exit(-3);
                }
            }

            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                    // fall through on purpose.
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;

                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.printf("brokerId of slave broker must be a positive integer");
                        System.exit(-3);
                    }
                    break;

                default:
                    break;
            }

            // Set HA port
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);


            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
            log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);

            final BrokerController controller = new BrokerController(//
                brokerConfig, //
                nettyServerConfig, //
                nettyClientConfig, //
                messageStoreConfig);

            // remember all configs to prevent discard
            controller.getConfiguration().registerConfig(properties);

            // Initialize broker controller.
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            // Register shutdown hook to shutdown various services.
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
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                            log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            log.info("No properties to set system environment");
            return;
        }

        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.DEFAULT_NAMESRV_ADDR_LOOKUP);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", "nsaddr");
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
