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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.util.Date;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.LoggerFactory;

public class ProxyStartup {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final ProxyStartAndShutdown PROXY_START_AND_SHUTDOWN = new ProxyStartAndShutdown();

    private static class ProxyStartAndShutdown extends AbstractStartAndShutdown {
        @Override
        public void appendStartAndShutdown(StartAndShutdown startAndShutdown) {
            super.appendStartAndShutdown(startAndShutdown);
        }
    }

    public static void main(String[] args) {
        try {
            ConfigurationManager.initEnv();
            initLogger();
            ConfigurationManager.intConfig();

            // init thread pool monitor for proxy.
            initThreadPoolMonitor();

            ThreadPoolExecutor executor = createServerExecutor();

            ServiceManager serviceManager = createServiceManager(null);

            // create grpcServer
            GrpcServer grpcServer = GrpcServerBuilder.newBuilder(executor)
                .addService(createServiceProcessor(serviceManager))
                .configInterceptor()
                .build();
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(grpcServer);

            // start servers one by one.
            PROXY_START_AND_SHUTDOWN.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("try to shutdown server");
                try {
                    PROXY_START_AND_SHUTDOWN.shutdown();
                } catch (Exception e) {
                    log.error("err when shutdown rmq-proxy", e);
                }
            }));
        } catch (Exception e) {
            System.err.println("find a unexpect err." + e);
            e.printStackTrace();
            log.error("find a unexpect err.", e);
            System.exit(1);
        }

        System.out.printf("%s%n", new Date() + " rmq-proxy startup successfully");
        log.info(new Date() + " rmq-proxy startup successfully");
    }

    private static ServiceManager createServiceManager(RPCHook rpcHook) {
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        ServiceManager serviceManager;

        if (ProxyMode.isClusterMode(proxyModeStr)) {
            serviceManager = ServiceManager.createForClusterMode(rpcHook);
        } else if (ProxyMode.isLocalMode(proxyModeStr)) {
            BrokerController brokerController = createBrokerController();
            StartAndShutdown brokerControllerWrapper = new StartAndShutdown() {
                @Override
                public void start() throws Exception {
                    brokerController.start();
                }

                @Override
                public void shutdown() throws Exception {
                    brokerController.shutdown();
                }
            };
            PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(brokerControllerWrapper);
            serviceManager = ServiceManager.createForLocalMode(brokerController, rpcHook);
        } else {
            throw new IllegalArgumentException("try to start grpc server with wrong mode, use 'local' or 'cluster'");
        }
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(serviceManager);
        return serviceManager;
    }

    private static GrpcMessagingApplication createServiceProcessor(ServiceManager serviceManager) {
        GrpcMessagingApplication application = GrpcMessagingApplication.create(serviceManager);
        PROXY_START_AND_SHUTDOWN.appendStartAndShutdown(application);
        return application;
    }

    private static BrokerController createBrokerController() {
        String[] brokerStartupArgs = new String[] {"-c", ConfigurationManager.getProxyConfig().getBrokerConfigPath()};
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    public static ThreadPoolExecutor createServerExecutor() {
        int threadPoolNums = ConfigurationManager.getProxyConfig().getGrpcThreadPoolNums();
        int threadPoolQueueCapacity = ConfigurationManager.getProxyConfig().getGrpcThreadPoolQueueCapacity();
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
        ThreadPoolMonitor.init();
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ThreadPoolMonitor.config(config.isEnablePrintJstack(), config.getPrintJstackInMillis());
    }

    public static void initLogger() throws JoranException {
        System.setProperty("brokerLogDir", "");
        System.setProperty(ClientLogger.CLIENT_LOG_USESLF4J, "true");

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        //https://logback.qos.ch/manual/configuration.html
        lc.setPackagingDataEnabled(false);
        configurator.doConfigure(ConfigurationManager.getProxyHome() + "/conf/logback_proxy.xml");
    }
}