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
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.tools.command.broker.BrokerStatsSubCommand;
import com.alibaba.rocketmq.tools.command.broker.UpdateBrokerConfigSubCommand;
import com.alibaba.rocketmq.tools.command.cluster.ClusterListSubCommand;
import com.alibaba.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import com.alibaba.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.ConsumerProgressSubCommand;
import com.alibaba.rocketmq.tools.command.consumer.DeleteSubscriptionGroupCommand;
import com.alibaba.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByIdSubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByKeySubCommand;
import com.alibaba.rocketmq.tools.command.message.QueryMsgByOffsetSubCommand;
import com.alibaba.rocketmq.tools.command.namesrv.DeleteKvConfigCommand;
import com.alibaba.rocketmq.tools.command.namesrv.DeleteProjectGroupCommand;
import com.alibaba.rocketmq.tools.command.namesrv.GetProjectGroupCommand;
import com.alibaba.rocketmq.tools.command.namesrv.UpdateKvConfigCommand;
import com.alibaba.rocketmq.tools.command.namesrv.UpdateProjectGroupCommand;
import com.alibaba.rocketmq.tools.command.namesrv.WipeWritePermSubCommand;
import com.alibaba.rocketmq.tools.command.offset.GetConsumerStatusCommand;
import com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeCommand;
import com.alibaba.rocketmq.tools.command.offset.ResetOffsetByTimeOldCommand;
import com.alibaba.rocketmq.tools.command.topic.DeleteTopicSubCommand;
import com.alibaba.rocketmq.tools.command.topic.TopicListSubCommand;
import com.alibaba.rocketmq.tools.command.topic.TopicRouteSubCommand;
import com.alibaba.rocketmq.tools.command.topic.TopicStatsSubCommand;
import com.alibaba.rocketmq.tools.command.topic.UpdateOrderConfCommand;
import com.alibaba.rocketmq.tools.command.topic.UpdateTopicSubCommand;


/**
 * mqadmin启动程序
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public class MQAdminStartup {
    protected static List<SubCommand> subCommandList = new ArrayList<SubCommand>();
    static {
        subCommandList.add(new UpdateTopicSubCommand());
        subCommandList.add(new DeleteTopicSubCommand());
        subCommandList.add(new UpdateSubGroupSubCommand());
        subCommandList.add(new DeleteSubscriptionGroupCommand());
        subCommandList.add(new UpdateBrokerConfigSubCommand());

        subCommandList.add(new TopicRouteSubCommand());
        subCommandList.add(new TopicStatsSubCommand());

        subCommandList.add(new BrokerStatsSubCommand());
        subCommandList.add(new QueryMsgByIdSubCommand());
        subCommandList.add(new QueryMsgByKeySubCommand());
        subCommandList.add(new QueryMsgByOffsetSubCommand());

        subCommandList.add(new ProducerConnectionSubCommand());
        subCommandList.add(new ConsumerConnectionSubCommand());
        subCommandList.add(new ConsumerProgressSubCommand());

        subCommandList.add(new ClusterListSubCommand());
        subCommandList.add(new TopicListSubCommand());

        subCommandList.add(new UpdateKvConfigCommand());
        subCommandList.add(new DeleteKvConfigCommand());

        subCommandList.add(new UpdateProjectGroupCommand());
        subCommandList.add(new DeleteProjectGroupCommand());
        subCommandList.add(new GetProjectGroupCommand());
        subCommandList.add(new WipeWritePermSubCommand());
        subCommandList.add(new ResetOffsetByTimeOldCommand());
        subCommandList.add(new ResetOffsetByTimeCommand());
        subCommandList.add(new GetConsumerStatusCommand());

        subCommandList.add(new UpdateOrderConfCommand());
    }


    public static void main(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

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
                        Options options = MixAll.buildCommandlineOptions(new Options());
                        options = cmd.buildCommandlineOptions(options);
                        if (options != null) {
                            MixAll.printCommandLineHelp("mqadmin " + cmd.commandName(), options);
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
                    Options options = MixAll.buildCommandlineOptions(new Options());
                    final CommandLine commandLine =
                            MixAll.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                                cmd.buildCommandlineOptions(options), new PosixParser());
                    if (null == commandLine) {
                        System.exit(-1);
                        return;
                    }

                    if (commandLine.hasOption('n')) {
                        String namesrvAddr = commandLine.getOptionValue('n');
                        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
                    }

                    cmd.execute(commandLine, options);
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
