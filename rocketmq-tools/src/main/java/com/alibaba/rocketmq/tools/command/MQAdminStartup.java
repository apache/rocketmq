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
package com.alibaba.rocketmq.tools.command;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.conflict.PackageConflictDetect;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.command.broker.BrokerStatusSubCommand;
import com.alibaba.rocketmq.tools.command.broker.CleanExpiredCQSubCommand;
import com.alibaba.rocketmq.tools.command.broker.UpdateBrokerConfigSubCommand;
import com.alibaba.rocketmq.tools.command.cluster.ClusterListSubCommand;
import com.alibaba.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import com.alibaba.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.ConsumerProgressSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.ConsumerStatusSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.DeleteSubscriptionGroupCommand;
import com.alibaba.rocketmq.tools.command.consumer.StartMonitoringSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand;
import com.alibaba.rocketmq.tools.command.message.CheckMsgSubCommand;
import com.alibaba.rocketmq.tools.command.message.PrintMessageSubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByIdSubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByKeySubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByOffsetSubCommand;
import com.alibaba.rocketmq.tools.command.namesrv.DeleteKvConfigCommand;
import com.alibaba.rocketmq.tools.command.namesrv.UpdateKvConfigCommand;
import com.alibaba.rocketmq.tools.command.namesrv.WipeWritePermSubCommand;
import com.alibaba.rocketmq.tools.command.offset.CloneGroupOffsetCommand;
import com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeCommand;
import com.alibaba.rocketmq.tools.command.stats.StatsAllSubCommand;
import com.alibaba.rocketmq.tools.command.topic.DeleteTopicSubCommand;
import com.alibaba.rocketmq.tools.command.topic.TopicListSubCommand;
import com.alibaba.rocketmq.tools.command.topic.TopicRouteSubCommand;
import com.alibaba.rocketmq.tools.command.topic.TopicStatusSubCommand;
import com.alibaba.rocketmq.tools.command.topic.UpdateOrderConfCommand;
import com.alibaba.rocketmq.tools.command.topic.UpdateTopicSubCommand;
import com.alibaba.rocketmq.tools.github.SyncDocsToGithubSubCommand;


/**
 * mqadmin启动程序
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public class MQAdminStartup {
    protected static List<SubCommand> subCommandList = new ArrayList<SubCommand>();


    public static void initCommand() {
        initCommand(new UpdateTopicSubCommand());
        initCommand(new DeleteTopicSubCommand());
        initCommand(new UpdateSubGroupSubCommand());
        initCommand(new DeleteSubscriptionGroupCommand());
        initCommand(new UpdateBrokerConfigSubCommand());

        initCommand(new TopicRouteSubCommand());
        initCommand(new TopicStatusSubCommand());

        initCommand(new BrokerStatusSubCommand());
        initCommand(new QueryMsgByIdSubCommand());
        initCommand(new QueryMsgByKeySubCommand());
        initCommand(new QueryMsgByOffsetSubCommand());
        initCommand(new PrintMessageSubCommand());

        initCommand(new ProducerConnectionSubCommand());
        initCommand(new ConsumerConnectionSubCommand());
        initCommand(new ConsumerProgressSubCommand());
        initCommand(new ConsumerStatusSubCommand());
        initCommand(new CloneGroupOffsetCommand());

        initCommand(new ClusterListSubCommand());
        initCommand(new TopicListSubCommand());

        initCommand(new UpdateKvConfigCommand());
        initCommand(new DeleteKvConfigCommand());

        initCommand(new WipeWritePermSubCommand());
        initCommand(new ResetOffsetByTimeCommand());

        initCommand(new UpdateOrderConfCommand());
        initCommand(new CleanExpiredCQSubCommand());

        initCommand(new StartMonitoringSubCommand());
        initCommand(new CheckMsgSubCommand());
        initCommand(new StatsAllSubCommand());

        initCommand(new SyncDocsToGithubSubCommand());
    }


    public static void initCommand(SubCommand command) {
        subCommandList.add(command);
    }


    public static void main(String[] args) {
        main0(args, null);
    }


    public static void main0(String[] args, RPCHook rpcHook) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        // 检测包冲突
        PackageConflictDetect.detectFastjson();

        initCommand();

        try {
            initLogback();
            switch (args.length) {
            case 0:
                printHelp();
                break;
            case 2:
                if (args[0].equals("help")) {
                    SubCommand cmd = findSubCommand(args[1]);
                    if (cmd != null) {
                        Options options = ServerUtil.buildCommandlineOptions(new Options());
                        options = cmd.buildCommandlineOptions(options);
                        if (options != null) {
                            ServerUtil.printCommandLineHelp("mqadmin " + cmd.commandName(), options);
                        }
                    }
                    else {
                        System.out.println("The sub command \'" + args[1] + "\' not exist.");
                    }
                    break;
                }
            case 1:
            default:
                SubCommand cmd = findSubCommand(args[0]);
                if (cmd != null) {
                    // 将main中的args转化为子命令的args（去除第一个参数）
                    String[] subargs = parseSubArgs(args);

                    // 解析命令行
                    Options options = ServerUtil.buildCommandlineOptions(new Options());
                    final CommandLine commandLine =
                            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                                cmd.buildCommandlineOptions(options), new PosixParser());
                    if (null == commandLine) {
                        System.exit(-1);
                        return;
                    }

                    if (commandLine.hasOption('n')) {
                        String namesrvAddr = commandLine.getOptionValue('n');
                        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
                    }

                    cmd.execute(commandLine, options, rpcHook);
                }
                else {
                    System.out.println("The sub command \'" + args[0] + "\' not exist.");
                }
                break;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void initLogback() throws JoranException {
        String rocketmqHome =
                System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

        // 初始化Logback
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(rocketmqHome + "/conf/logback_tools.xml");
    }


    private static String[] parseSubArgs(String[] args) {
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }


    private static SubCommand findSubCommand(final String name) {
        for (SubCommand cmd : subCommandList) {
            if (cmd.commandName().toUpperCase().equals(name.toUpperCase())) {
                return cmd;
            }
        }

        return null;
    }


    private static void printHelp() {
        System.out.println("The most commonly used mqadmin commands are:");

        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-20s %s\n", cmd.commandName(), cmd.commandDesc());
        }

        System.out.println("\nSee 'mqadmin help <command>' for more information on a specific command.");
    }
}
