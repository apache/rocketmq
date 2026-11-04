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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

/**
 * Deletes a batch of subscription groups with a single request per broker, using the broker side
 * {@code DELETE_SUBSCRIPTION_GROUP_LIST} request. This is the file driven counterpart of
 * {@link DeleteSubscriptionGroupCommand}: it exists because an operator who migrated a large number
 * of groups needs a way to clean them up without a custom client, and looping over
 * {@code deleteSubGroup} would issue one request per group.
 */
public class DeleteSubscriptionGroupListSubCommand implements SubCommand {

    /**
     * The broker removes the groups one by one behind a rate limiter
     * ({@code BrokerConfig#batchDeleteSubscriptionGroupMaxRate}, 10 per second by default), so a
     * large input file takes far longer than the 5s timeout the admin client uses by default.
     * Give the request a generous deadline instead of aborting a batch the broker still processes.
     */
    private static final long DEFAULT_TIMEOUT_MILLIS = 60_000L;

    @Override
    public String commandName() {
        return "deleteSubscriptionGroupList";
    }

    @Override
    public String commandDesc() {
        return "Delete subscription groups in batch from broker.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        final OptionGroup optionGroup = new OptionGroup();
        Option opt = new Option("b", "brokerAddr", true, "delete subscription groups from which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete subscription groups from which cluster");
        optionGroup.addOption(opt);
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("f", "filename", true,
            "Path to a file with a JSON array of subscription group names");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "removeOffset", true, "whether to remove the consumer offsets of the groups");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        final DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        final String fileName = commandLine.getOptionValue('f').trim();
        boolean cleanOffset = false;
        if (commandLine.hasOption('r')) {
            cleanOffset = Boolean.parseBoolean(commandLine.getOptionValue('r').trim());
        }

        try {
            final Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                System.out.printf("the file path %s does not exists%n", fileName);
                return;
            }
            final byte[] groupNameListBytes = Files.readAllBytes(filePath);
            final List<String> groupNames = JSON.parseArray(groupNameListBytes, String.class);
            if (null == groupNames || groupNames.isEmpty()) {
                return;
            }

            if (commandLine.hasOption('b')) {
                final String brokerAddress = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                defaultMQAdminExt.deleteSubscriptionGroupList(brokerAddress, groupNames, cleanOffset,
                    DEFAULT_TIMEOUT_MILLIS);

                System.out.printf("submit batch of %d subscription groups to %s success, please check the result later.%n",
                    groupNames.size(), brokerAddress);
                return;

            } else if (commandLine.hasOption('c')) {
                final String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddress : masterSet) {
                    defaultMQAdminExt.deleteSubscriptionGroupList(brokerAddress, groupNames, cleanOffset,
                        DEFAULT_TIMEOUT_MILLIS);

                    System.out.printf("submit batch of %d subscription groups to %s in cluster %s success, please check the result later.%n",
                        groupNames.size(), brokerAddress, clusterName);
                }

                // The batch request only drops the group configs. The retry and dead letter topics
                // derived from the group names live in the topic config, so they have to be removed
                // separately, exactly like the single item deleteSubGroup command does in cluster mode.
                for (String groupName : groupNames) {
                    try {
                        defaultMQAdminExt.deleteTopic(MixAll.RETRY_GROUP_TOPIC_PREFIX + groupName, clusterName);
                        defaultMQAdminExt.deleteTopic(MixAll.DLQ_GROUP_TOPIC_PREFIX + groupName, clusterName);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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
