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
package org.apache.rocketmq.snode;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_ENABLE;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.slf4j.LoggerFactory;

public class SnodeStartup {
    private static InternalLogger log;
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;

    public static void main(String[] args) throws IOException, JoranException {
        startup(createSnodeController(args));
    }

    public static SnodeController startup(SnodeController controller) {
        try {
            controller.start();

            String tip = "The snode[" + controller.getSnodeConfig().getSnodeName() + ", "
                + controller.getSnodeConfig().getSnodeIP1() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getSnodeConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getSnodeConfig().getNamesrvAddr();
            }
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    public static SnodeController createSnodeController(String[] args) throws IOException, JoranException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("snode", args, buildCommandlineOptions(options),
            new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final SnodeConfig snodeConfig = new SnodeConfig();
        final ServerConfig nettyServerConfig = new ServerConfig();
        final ClientConfig nettyClientConfig = new ClientConfig();

        nettyServerConfig.setListenPort(snodeConfig.getListenPort());
        nettyServerConfig.setListenPort(11911);
        nettyClientConfig.setUseTLS(Boolean.parseBoolean(System.getProperty(TLS_ENABLE,
            String.valueOf(TlsSystemConfig.tlsMode == TlsMode.ENFORCING))));

        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                configFile = file;
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, snodeConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);
                in.close();
            }
        }
        if (null == snodeConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(snodeConfig.getRocketmqHome() + "/conf/logback_snode.xml");
        log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

        MixAll.printObjectProperties(log, snodeConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        final SnodeController snodeController = new SnodeController(
            nettyServerConfig,
            nettyClientConfig,
            snodeConfig);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        snodeController.shutdown();
                    }
                }
            }
        }));
        return snodeController;
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "SNode config properties file");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }
}

