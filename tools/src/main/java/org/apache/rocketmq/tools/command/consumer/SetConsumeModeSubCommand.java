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
package org.apache.rocketmq.tools.command.consumer;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;


public class SetConsumeModeSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "setConsumeMode";
    }


    @Override
    public String commandDesc() {
        return "Set consume message mode. pull/pop etc.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create subscription group to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "create subscription group to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topicName", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("m", "mode", true, "consume mode. PULL/POP");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("q", "popShareQueueNum", true, "num of queue which share in pop mode");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
            throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setVipChannelEnabled(false);

        try {

            String topicName = commandLine.getOptionValue('t').trim();
            String groupName = commandLine.getOptionValue('g').trim();

            MessageRequestMode mode = MessageRequestMode.valueOf(commandLine.getOptionValue('m').trim());

            int popShareQueueNum = 0;
            if (commandLine.hasOption('q')) {
                popShareQueueNum = Integer.parseInt(commandLine.getOptionValue('q')
                        .trim());
            }

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName, addr);

                defaultMQAdminExt.setMessageRequestMode(addr, topicName, groupName, mode, popShareQueueNum, 5000);
                for (Connection connection : consumerConnection.getConnectionSet()) {
                    defaultMQAdminExt.notifyMessageRequestModeToClient(connection.getClientAddr(), topicName, groupName, mode, 5000);
                    System.out.printf("notify changes to clientAddr:[%s]", connection.getClientAddr());
                }
                System.out.printf("set consume mode to %s success.%n", addr);
                System.out.printf("topic[%s] group[%s] consume mode[%s] popShareQueueNum[%d]",
                        topicName, groupName, mode.toString(), popShareQueueNum);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                ConsumerConnection consumerConnection = null;

                defaultMQAdminExt.start();
                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    try {
                        if (consumerConnection == null) {
                            consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName, addr);
                        }
                        defaultMQAdminExt.setMessageRequestMode(addr, topicName, groupName, mode, popShareQueueNum, 5000);
                        System.out.printf("set consume mode to %s success.%n", addr);
                    } catch (Exception e) {
                        e.printStackTrace();
                        Thread.sleep(1000 * 1);
                    }
                }
                if (consumerConnection != null) {
                    for (Connection connection : consumerConnection.getConnectionSet()) {
                        defaultMQAdminExt.notifyMessageRequestModeToClient(connection.getClientAddr(), topicName, groupName, mode, 5000);
                        System.out.printf("notify changes to clientAddr:[%s]", connection.getClientAddr());
                    }
                }
                System.out.printf("topic[%s] group[%s] consume mode[%s] popShareQueueNum[%d]",
                        topicName, groupName, mode.toString(), popShareQueueNum);
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
