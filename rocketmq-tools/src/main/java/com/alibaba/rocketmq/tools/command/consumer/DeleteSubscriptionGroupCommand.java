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
package com.alibaba.rocketmq.tools.command.consumer;

import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;
import com.alibaba.rocketmq.tools.command.topic.DeleteTopicSubCommand;


/**
 * 删除订阅组配置命令
 * 
 * @author manhong.yqd<manhong.yqd@alibaba-inc.com>
 * @since 2013-8-22
 */
public class DeleteSubscriptionGroupCommand implements SubCommand {
    @Override
    public String commandName() {
        return "deleteSubGroup";
    }


    @Override
    public String commandDesc() {
        return "Delete subscription group from broker.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "delete subscription group from which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete subscription group from which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "subscription group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // groupName
            String groupName = commandLine.getOptionValue('g').trim();

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                adminExt.start();

                adminExt.deleteSubscriptionGroup(addr, groupName);
                System.out.printf("delete subscription group [%s] from broker [%s] success.\n", groupName,
                    addr);

                return;
            }
            else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                adminExt.start();

                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
                for (String master : masterSet) {
                    adminExt.deleteSubscriptionGroup(master, groupName);
                    System.out.printf(
                        "delete subscription group [%s] from broker [%s] in cluster [%s] success.\n",
                        groupName, master, clusterName);
                }

                // 删除%RETRY%打头的Topic
                try {
                    DeleteTopicSubCommand.deleteTopic(adminExt, clusterName, MixAll.RETRY_GROUP_TOPIC_PREFIX
                            + groupName);
                    DeleteTopicSubCommand.deleteTopic(adminExt, clusterName, MixAll.DLQ_GROUP_TOPIC_PREFIX
                            + groupName);
                }
                catch (Exception e) {
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            adminExt.shutdown();
        }
    }
}
