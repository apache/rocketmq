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
package org.apache.rocketmq.filtersrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FiltersrvStartup {
    public static Logger log;

    public static void main(String[] args) {
        start(createController(args));
    }

    public static FiltersrvController start(FiltersrvController controller) {

        try {
            controller.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        String tip = "The Filter Server boot success, " + controller.localAddr();
        log.info(tip);
        System.out.printf("%s%n", tip);

        return controller;
    }

    public static FiltersrvController createController(String[] args) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            NettySystemConfig.socketSndbufSize = 65535;
        }

        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            NettySystemConfig.socketRcvbufSize = 1024;
        }

        try {
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqfiltersrv", args, buildCommandlineOptions(options),
                    new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            final FiltersrvConfig filtersrvConfig = new FiltersrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();

            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    Properties properties = new Properties();
                    properties.load(in);
                    MixAll.properties2Object(properties, filtersrvConfig);
                    System.out.printf("load config properties file OK, " + file + "%n");
                    in.close();

                    String port = properties.getProperty("listenPort");
                    if (port != null) {
                        filtersrvConfig.setConnectWhichBroker(String.format("127.0.0.1:%s", port));
                    }
                }
            }

            nettyServerConfig.setListenPort(0);
            nettyServerConfig.setServerAsyncSemaphoreValue(filtersrvConfig.getFsServerAsyncSemaphoreValue());
            nettyServerConfig.setServerCallbackExecutorThreads(filtersrvConfig
                .getFsServerCallbackExecutorThreads());
            nettyServerConfig.setServerWorkerThreads(filtersrvConfig.getFsServerWorkerThreads());

            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, filtersrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), filtersrvConfig);
            if (null == filtersrvConfig.getRocketmqHome()) {
                System.out.printf("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                    + " variable in your environment to match the location of the RocketMQ installation%n");
                System.exit(-2);
            }

            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(filtersrvConfig.getRocketmqHome() + "/conf/logback_filtersrv.xml");
            log = LoggerFactory.getLogger(LoggerName.FILTERSRV_LOGGER_NAME);

            final FiltersrvController controller =
                new FiltersrvController(filtersrvConfig, nettyServerConfig);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);

                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
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

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Filter server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
