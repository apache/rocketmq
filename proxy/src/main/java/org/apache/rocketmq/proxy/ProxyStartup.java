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

package org.apache.rocketmq.proxy;

import com.google.common.collect.Lists;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.config.Configuration;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.admin.ProxyAdminAuthInterceptor;
import org.apache.rocketmq.proxy.grpc.admin.ProxyAdminBindableService;
import org.apache.rocketmq.proxy.grpc.admin.ProxyAdminGrpcService;
import org.apache.rocketmq.proxy.grpc.admin.ProxyAdminMetricsManager;
import org.apache.rocketmq.proxy.grpc.admin.RouteChangeNotifier;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.RemotingProtocolServer;
import org.apache.rocketmq.proxy.service.admin.DefaultProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ProxyStartup {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final ProxyStartAndShutdown PROXY_START_AND_SHUTDOWN = new ProxyStartAndShutdown();

    private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
        @Override
        public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
            super.appendStartAndShutdown(startAndShutdown);
        }
    }

    public static void main(String[] args) {
        try {
            // parse argument from command line
            CommandLineArgument commandLineArgument = parseCommandLineArgument(args);
            initConfiguration(commandLineArgument);

            // init thread pool monitor for proxy.
            initThreadPoolMonitor();

            ThreadPoolExecutor executor = createServerExecutor();

            MessagingProcessor messagingProcessor = createMessagingProcessor();

            // tls cert update
            TlsCertificateManager tlsCertificateManager = new TlsCertificateManager();
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(tlsCertificateManager);

            // create grpcServer
            GrpcMessagingApplication grpcMessagingApplication = createServiceProcessor(messagingProcessor);

            GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor,
                    ConfigurationManager.getProxyConfig().getGrpcServerPort(), tlsCertificateManager)
                .addService(grpcMessagingApplication)
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .configInterceptor()
                .shutdownTime(ConfigurationManager.getProxyConfig().getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS)
                .build();
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(grpcServer);

            // create admin grpc server (RIP-2)
            GrpcServer adminGrpcServer = createAdminGrpcServer(executor, grpcMessagingApplication, messagingProcessor, tlsCertificateManager);
            if (adminGrpcServer != null) {
                PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(adminGrpcServer);
            }

            RemotingProtocolServer remotingServer = new RemotingProtocolServer(messagingProcessor, tlsCertificateManager);
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(remotingServer);

            // start servers one by one.
            PROXY_START_AND_SHUTDOWN.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("try to shutdown server");
                try {
                    PROXY_START_AND_SHUTDOWN.preShutdown();
                    PROXY_START_AND_SHUTDOWN.shutdown();
                } catch (Exception e) {
                    log.error("err when shutdown rocketmq-proxy", e);
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
            log.error("find an unexpect err.", e);
            System.exit(1);
        }

        System.out.printf("%s%n", new Date() + " rocketmq-proxy startup successfully");
        log.info(new Date() + " rocketmq-proxy startup successfully");
    }

    protected static void initConfiguration(CommandLineArgument commandLineArgument) throws Exception {
        if (StringUtils.isNotBlank(commandLineArgument.getProxyConfigPath())) {
            System.setProperty(Configuration.CONFIG_PATH_PROPERTY, commandLineArgument.getProxyConfigPath());
        }
        ConfigurationManager.initEnv();
        ConfigurationManager.initConfig();
        setConfigFromCommandLineArgument(commandLineArgument);
        log.info("Current configuration: " + ConfigurationManager.formatProxyConfig());

    }

    protected static CommandLineArgument parseCommandLineArgument(String[] args) {
        CommandLine commandLine = ServerUtil.parseCmdLine("mqproxy", args,
            buildCommandlineOptions(), new DefaultParser());
        if (commandLine == null) {
            throw new RuntimeException("parse command line argument failed");
        }

        CommandLineArgument commandLineArgument = new CommandLineArgument();
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), commandLineArgument);
        return commandLineArgument;
    }

    private static Options buildCommandlineOptions() {
        Options options = ServerUtil.buildCommandlineOptions(new Options());

        Option opt = new Option("bc", "brokerConfigPath", true, "Broker config file path for local mode");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pc", "proxyConfigPath", true, "Proxy config file path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("pm", "proxyMode", true, "Proxy run in local or cluster mode");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private static void setConfigFromCommandLineArgument(CommandLineArgument commandLineArgument) {
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

    protected static MessagingProcessor createMessagingProcessor() {
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        MessagingProcessor messagingProcessor;

        if (ProxyMode.isClusterMode(proxyModeStr)) {
            messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
            ProxyMetricsManager proxyMetricsManager = ProxyMetricsManager.initClusterMode(ConfigurationManager.getProxyConfig());
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(proxyMetricsManager);
        } else if (ProxyMode.isLocalMode(proxyModeStr)) {
            BrokerController brokerController = createBrokerController();
            ProxyMetricsManager.initLocalMode(brokerController.getBrokerMetricsManager(), ConfigurationManager.getProxyConfig());
            StartAndShutdown brokerControllerWrapper = new StartAndShutdown() {
                @Override
                public void start() throws Exception {
                    brokerController.start();
                    String tip = "The broker[" + brokerController.getBrokerConfig().getBrokerName() + ", "
                        + brokerController.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
                    if (null != brokerController.getBrokerConfig().getNamesrvAddr()) {
                        tip += " and name server is " + brokerController.getBrokerConfig().getNamesrvAddr();
                    }
                    log.info(tip);
                }

                @Override
                public void shutdown() throws Exception {
                    brokerController.shutdown();
                }
            };
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(brokerControllerWrapper);
            messagingProcessor = DefaultMessagingProcessor.createForLocalMode(brokerController);
        } else {
            throw new IllegalArgumentException("try to start grpc server with wrong mode, use 'local' or 'cluster'");
        }
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(messagingProcessor);
        return messagingProcessor;
    }

    private static GrpcMessagingApplication createServiceProcessor(MessagingProcessor messagingProcessor) {
        GrpcMessagingApplication application = GrpcMessagingApplication.create(messagingProcessor);
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(application);
        return application;
    }

    /**
     * Create the Admin gRPC Server for RIP-2 Proxy Admin interface.
     * <p>
     * The admin server runs on an independent port (default 8082) to isolate
     * control plane traffic from data plane traffic. It provides:
     * - ListClients: paginated listing of online clients
     * - DescribeClient: detailed client information
     * - ListClientsByGroup: clients filtered by consumer group
     * - ListClientsByTopic: clients filtered by subscribed topic
     * <p>
     * All admin RPCs are protected by ACL 2.0 authentication and authorization.
     *
     * @return GrpcServer for admin, or null if admin is disabled
     */
    private static GrpcServer createAdminGrpcServer(ThreadPoolExecutor executor,
        GrpcMessagingApplication grpcMessagingApplication,
        MessagingProcessor messagingProcessor, TlsCertificateManager tlsCertificateManager) {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        if (!config.isProxyAdminEnabled()) {
            log.info("Proxy admin gRPC server is disabled by configuration");
            return null;
        }

        try {
            // Get channel manager and settings manager from the data plane
            GrpcChannelManager grpcChannelManager = grpcMessagingApplication.getGrpcChannelManager();
            GrpcClientSettingsManager grpcClientSettingsManager = grpcMessagingApplication.getGrpcClientSettingsManager();

            // Initialize admin OTel metrics (RIP-2 §5.4.5 / §8.6)
            ProxyAdminMetricsManager.init(ProxyMetricsManager.getMeter());

            // Create admin service layer
            ProxyAdminClientService adminClientService = new DefaultProxyAdminClientService(
                grpcChannelManager, grpcClientSettingsManager);

            // Wire ReceiptHandleManager for POP diagnostics (RIP-2 M3)
            ((DefaultProxyAdminClientService) adminClientService).setReceiptHandleManager(
                messagingProcessor.getReceiptHandleManager());

            // Wire admin service into telemetry pipeline for heartbeat recording (RIP-2 §5.2.2)
            grpcMessagingApplication.setProxyAdminClientService(adminClientService);

            // Create gRPC service with isolated thread pool
            ProxyAdminGrpcService adminGrpcService;

            // Wire up RouteChangeNotifier for SubscribeRouteEvents streaming RPC
            TopicRouteService topicRouteService = messagingProcessor.getTopicRouteService();
            if (topicRouteService != null) {
                RouteChangeNotifier routeChangeNotifier = new RouteChangeNotifier(topicRouteService);
                topicRouteService.addRouteRefreshListener(routeChangeNotifier);
                adminGrpcService = new ProxyAdminGrpcService(
                    adminClientService, config.getProxyAdminThreadPoolNums(), routeChangeNotifier);
                PROXY_START_AND_SHUTDOWN.appendShutdown(routeChangeNotifier::shutdown);
                log.info("RouteChangeNotifier wired: listening for route refresh events from TopicRouteService");
            } else {
                adminGrpcService = new ProxyAdminGrpcService(
                    adminClientService, config.getProxyAdminThreadPoolNums());
                log.warn("TopicRouteService not available, SubscribeRouteEvents RPC will return UNAVAILABLE");
            }

            // Create bindable service wrapper
            ProxyAdminBindableService adminBindableService = new ProxyAdminBindableService(adminGrpcService);

            // Build admin gRPC server on independent port
            int adminPort = config.getProxyAdminServerPort();
            GrpcServerBuilder adminServerBuilder = GrpcServerBuilder.newBuilder(
                    executor, adminPort, tlsCertificateManager)
                .addService(adminBindableService)
                .shutdownTime(config.getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS);

            // Add ACL 2.0 auth interceptor if auth is configured
            AuthConfig authConfig = ConfigurationManager.getAuthConfig();
            if (authConfig != null && (authConfig.isAuthenticationEnabled() || authConfig.isAuthorizationEnabled())) {
                ProxyAdminAuthInterceptor authInterceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
                adminServerBuilder.appendInterceptor(authInterceptor);
                log.info("Proxy admin gRPC server: ACL 2.0 authentication/authorization enabled");
            }

            // Add standard interceptors
            adminServerBuilder.configInterceptor();

            GrpcServer adminGrpcServer = adminServerBuilder.build();
            log.info("Proxy admin gRPC server configured on port: {}, threadPoolNums: {}, maxPageSize: {}",
                adminPort, config.getProxyAdminThreadPoolNums(), config.getProxyAdminMaxPageSize());

            // Register admin service shutdown
            PROXY_START_AND_SHUTDOWN.appendShutdown(adminGrpcService::shutdown);

            return adminGrpcServer;
        } catch (Exception e) {
            log.error("Failed to create proxy admin gRPC server", e);
            return null;
        }
    }

    protected static BrokerController createBrokerController() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        List<String> brokerStartupArgList = Lists.newArrayList("-c", config.getBrokerConfigPath());
        if (StringUtils.isNotBlank(config.getNamesrvAddr())) {
            brokerStartupArgList.add("-n");
            brokerStartupArgList.add(config.getNamesrvAddr());
        }
        String[] brokerStartupArgs = brokerStartupArgList.toArray(new String[0]);
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    public static ThreadPoolExecutor createServerExecutor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        int threadPoolNums = config.getGrpcThreadPoolNums();
        int threadPoolQueueCapacity = config.getGrpcThreadPoolQueueCapacity();
        ThreadPoolExecutor executor = ThreadPoolMonitor.createAndMonitor(
            threadPoolNums,
            threadPoolNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            threadPoolQueueCapacity
        );
        PROXY_START_AND_SHUTDOWN.appendShutdown(executor::shutdown);
        return executor;
    }

    public static void initThreadPoolMonitor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ThreadPoolMonitor.config(
            LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME),
            LoggerFactory.getLogger(LoggerName.PROXY_WATER_MARK_LOGGER_NAME),
            config.isEnablePrintJstack(), config.getPrintJstackInMillis(),
            config.getPrintThreadPoolStatusInMillis());
        ThreadPoolMonitor.init();
    }
}
