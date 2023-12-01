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
 * limitations under the License. ...TI 2
 */
package org.apache.rocketmq.controller;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

public class ControllerStartup {

    private static Logger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static ControllerManager main0(String[] args) {

        try {
            ControllerManager controller = createControllerManager(args);
            start(controller);
            String tip = "The Controller Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static ControllerManager createControllerManager(String[] args) throws IOException {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("mqcontroller", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        final ControllerConfig controllerConfig = new ControllerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        final NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(19876);

        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, controllerConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        if (commandLine.hasOption('p')) {
            Logger console = LoggerFactory.getLogger(LoggerName.CONTROLLER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, controllerConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            MixAll.printObjectProperties(console, nettyClientConfig);
            System.exit(0);
        }

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), controllerConfig);

        if (StringUtils.isEmpty(controllerConfig.getRocketmqHome())) {
            System.out.printf("Please set the %s or %s variable in your environment!%n", MixAll.ROCKETMQ_HOME_ENV, MixAll.ROCKETMQ_HOME_PROPERTY);
            System.exit(-1);
        }

        log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, controllerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        final ControllerManager controllerManager = new ControllerManager(controllerConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controllerManager.getConfiguration().registerConfig(properties);

        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        controller.start();

        return controller;
    }

    public static void shutdown(final ControllerManager controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Controller config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

}
