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

import com.alibaba.fastjson2.JSON;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateTopicListSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateTopicList";
    }

    @Override
    public String commandDesc() {
        return "create or update topic in batch";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        final OptionGroup optionGroup = new OptionGroup();
        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        optionGroup.addOption(opt);
        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        optionGroup.addOption(opt);
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("f", "filename", true, "Path to a file with list of org.apache.rocketmq.common.TopicConfig in json format");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        final DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        final String fileName = commandLine.getOptionValue('f').trim();


        try {
            final Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                System.out.printf("the file path %s does not exists%n", fileName);
                return;
            }
            final byte[] topicConfigListBytes = Files.readAllBytes(filePath);
            final List<TopicConfig> topicConfigs = JSON.parseArray(topicConfigListBytes, TopicConfig.class);
            if (null == topicConfigs || topicConfigs.isEmpty()) {
                return;
            }

            if (commandLine.hasOption('b')) {
                String brokerAddress = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdateTopicConfigList(brokerAddress, topicConfigs);

                System.out.printf("submit batch of topic config to %s success, please check the result later.%n",
                    brokerAddress);
                return;

            } else if (commandLine.hasOption('c')) {
                final String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddress : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfigList(brokerAddress, topicConfigs);

                    System.out.printf("submit batch of topic config to %s success, please check the result later.%n",
                        brokerAddress);
                }
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
