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
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

/**
 * Deletes a batch of topics from a single broker with {@code DELETE_TOPIC_IN_BROKER_LIST}. It is the
 * file driven counterpart of the single item delete path and lets an operator clean up a large topic
 * list without issuing one remoting request per topic.
 *
 * <p>Note that the broker side batch delete only drops the topic config from the broker itself.
 * Unlike {@code deleteTopic} it does not unregister the route from the name servers, so when topics
 * are removed by cluster name and the routes should disappear as well, the operator still has to
 * clean the name server part separately.
 */
public class DeleteTopicListSubCommand implements SubCommand {

    /**
     * The broker removes the topics one by one behind a rate limiter
     * ({@code BrokerConfig#batchDeleteTopicMaxRate}, 10 per second by default), so a large input
     * file takes far longer than the 5s timeout the admin client uses by default. Give the request a
     * generous deadline instead of aborting a batch the broker is still working through.
     */
    private static final long DEFAULT_TIMEOUT_MILLIS = 60_000L;

    @Override
    public String commandName() {
        return "deleteTopicList";
    }

    @Override
    public String commandDesc() {
        return "Delete topics in batch from broker.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        final OptionGroup optionGroup = new OptionGroup();
        Option opt = new Option("b", "brokerAddr", true, "delete topics from which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete topics from which cluster");
        optionGroup.addOption(opt);
        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("f", "filename", true, "Path to a file with a JSON array of topic names");
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
            final byte[] topicListBytes = Files.readAllBytes(filePath);
            final List<String> topicNames = JSON.parseArray(topicListBytes, String.class);
            if (null == topicNames || topicNames.isEmpty()) {
                return;
            }

            if (commandLine.hasOption('b')) {
                final String brokerAddress = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                defaultMQAdminExt.deleteTopicInBrokerList(brokerAddress, topicNames, DEFAULT_TIMEOUT_MILLIS);

                System.out.printf("submit batch of %d topics to %s success, please check the result later.%n",
                    topicNames.size(), brokerAddress);
                return;

            } else if (commandLine.hasOption('c')) {
                final String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddress : masterSet) {
                    defaultMQAdminExt.deleteTopicInBrokerList(brokerAddress, topicNames, DEFAULT_TIMEOUT_MILLIS);

                    System.out.printf("submit batch of %d topics to %s in cluster %s success, please check the result later.%n",
                        topicNames.size(), brokerAddress, clusterName);
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