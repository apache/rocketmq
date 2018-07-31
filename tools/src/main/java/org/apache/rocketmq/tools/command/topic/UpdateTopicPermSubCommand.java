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
package org.apache.rocketmq.tools.command.topic;

import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateTopicPermSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopicPerm";
    }

    @Override
    public String commandDesc() {
        return "Update topic perm";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(2|4|6), intro[2:W; 4:R; 6:RW]");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            TopicConfig topicConfig = new TopicConfig();

            String topic = commandLine.getOptionValue('t').trim();
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            assert topicRouteData != null;
            List<QueueData> queueDatas = topicRouteData.getQueueDatas();
            assert queueDatas != null && queueDatas.size() > 0;

            QueueData queueData = queueDatas.get(0);
            topicConfig.setTopicName(topic);
            topicConfig.setWriteQueueNums(queueData.getWriteQueueNums());
            topicConfig.setReadQueueNums(queueData.getReadQueueNums());
            topicConfig.setPerm(queueData.getPerm());
            topicConfig.setTopicSysFlag(queueData.getTopicSynFlag());

            //new perm
            int perm = Integer.parseInt(commandLine.getOptionValue('p').trim());
            int oldPerm = topicConfig.getPerm();
            if (perm == oldPerm) {
                System.out.printf("new perm equals to the old one!%n");
                return;
            }
            topicConfig.setPerm(perm);
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                System.out.printf("update topic perm from %s to %s in %s success.%n", oldPerm, perm, addr);
                System.out.printf("%s%n", topicConfig);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                    System.out.printf("update topic perm from %s to %s in %s success.%n", oldPerm, perm, addr);
                }
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
