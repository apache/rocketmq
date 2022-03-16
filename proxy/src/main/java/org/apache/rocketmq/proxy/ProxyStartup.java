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
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.common.ProxyMode;
import org.apache.rocketmq.proxy.grpc.service.ClusterGrpcService;
import org.apache.rocketmq.proxy.grpc.service.GrpcForwardService;
import org.apache.rocketmq.proxy.grpc.service.LocalGrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyStartup {

    private static final Logger log = LoggerFactory.getLogger(ProxyStartup.class);

    public static void main(String[] args) {
        try {
            ConfigurationManager.initEnv();
            initLogger();
            ConfigurationManager.intConfig();

            // init thread pool monitor for proxy.
            initThreadPoolMonitor();

            // create and start grpcServer
            GrpcServer grpcServer = createGrpcServer();
            grpcServer.start();

            // health check server
            final HealthCheckServer healthCheckServer = new HealthCheckServer();
            healthCheckServer.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("try to shutdown server");

                try {
                    healthCheckServer.shutdown();
                    Thread.sleep(TimeUnit.SECONDS.toMillis(ConfigurationManager.getProxyConfig().getWaitAfterStopHealthCheckInSeconds()));
                } catch (Exception e) {
                    log.error("err when shutdown healthCheckServer", e);
                }

                try {
                    grpcServer.shutdown();
                } catch (Exception e) {
                    log.error("err when shutdown grpc server", e);
                }
            }));
        } catch (Exception e) {
            System.err.println("find a unexpect err." + e);
            e.printStackTrace();
            log.error("find a unexpect err.", e);
            System.exit(1);
        }

        System.out.printf("%s%n", new Date() + " rmq-proxy startup successfully");
        log.info(new Date() + "rmq-proxy startup successfully");
    }

    private static GrpcServer createGrpcServer() throws RuntimeException {
        GrpcForwardService grpcService;
        String proxyModeStr = ConfigurationManager.getProxyConfig().getProxyMode();
        if (ProxyMode.isClusterMode(proxyModeStr)) {
            grpcService = new ClusterGrpcService();
        } else if (ProxyMode.isLocalMode(proxyModeStr)) {
            BrokerController brokerController = createBrokerController();
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