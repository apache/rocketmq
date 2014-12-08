/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.filtersrv;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.conflict.PackageConflictDetect;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.netty.NettySystemConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.srvutil.ServerUtil;


/**
 * Filter server 启动入口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2014-4-10
 */
public class FiltersrvStartup {
    public static Logger log;


    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Filter server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static void main(String[] args) {
        start(createController(args));
    }


    public static FiltersrvController start(FiltersrvController controller) {
        // 启动服务
        try {
            controller.start();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        String tip = "The Filter Server boot success, " + controller.localAddr();
        log.info(tip);
        System.out.println(tip);

        return controller;
    }


    public static FiltersrvController createController(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        // Socket发送缓冲区大小
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.SocketSndbufSize = 65535;
        }

        // Socket接收缓冲区大小
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.SocketRcvbufSize = 1024;
        }

        try {
            // 检测包冲突
            PackageConflictDetect.detectFastjson();

            // 解析命令行
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            final CommandLine commandLine =
                    ServerUtil.parseCmdLine("mqfiltersrv", args, buildCommandlineOptions(options),
                        new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            // 初始化配置文件
            final FiltersrvConfig filtersrvConfig = new FiltersrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();

            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    Properties properties = new Properties();
                    properties.load(in);
                    MixAll.properties2Object(properties, filtersrvConfig);
                    System.out.println("load config properties file OK, " + file);
                    in.close();

                    String port = properties.getProperty("listenPort");
                    if (port != null) {
                        filtersrvConfig.setConnectWhichBroker(String.format("127.0.0.1:%s", port));
                    }
                }
            }

            // 强制设置为0，自动分配端口号
            nettyServerConfig.setListenPort(0);

            nettyServerConfig.setServerAsyncSemaphoreValue(filtersrvConfig.getFsServerAsyncSemaphoreValue());
            nettyServerConfig.setServerCallbackExecutorThreads(filtersrvConfig
                .getFsServerCallbackExecutorThreads());
            nettyServerConfig.setServerWorkerThreads(filtersrvConfig.getFsServerWorkerThreads());

            // 打印默认配置
            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, filtersrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), filtersrvConfig);

            if (null == filtersrvConfig.getRocketmqHome()) {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            // 初始化Logback
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(filtersrvConfig.getRocketmqHome() + "/conf/logback_filtersrv.xml");
            log = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);

            // 初始化服务控制对象
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
        }
        catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }
}
