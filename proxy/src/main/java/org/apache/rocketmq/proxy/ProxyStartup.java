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
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyMode;
import org.apache.rocketmq.proxy.grpc.v2.service.ClusterGrpcService;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcForwardService;
import org.apache.rocketmq.proxy.grpc.v2.service.LocalGrpcService;
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

            // create grpcServer
            GrpcServer grpcServer = createGrpcServer();
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

    private static GrpcServer createGrpcServer() throws Exception {
        GrpcForwardService grpcService;
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        if (ProxyMode.isClusterMode(proxyModeStr)) {
            grpcService = new ClusterGrpcService();
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
            grpcService = new LocalGrpcService(brokerController);
        } else {
            throw new IllegalArgumentException("try to start grpc server with wrong mode, use 'local' or 'cluster'");
        }

        return new GrpcServer(grpcService);
    }

    private static BrokerController createBrokerController() {
        String[] brokerStartupArgs = new String[] {"-c", ConfigurationManager.getProxyConfig().getBrokerConfigPath()};
        return BrokerStartup.createBrokerController(brokerStartupArgs);
    }

    private static void initThreadPoolMonitor() {
        ThreadPoolMonitor.init();
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        ThreadPoolMonitor.config(config.isEnablePrintJstack(), config.getPrintJstackInMillis());
    }

    private static void initLogger() throws JoranException {
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