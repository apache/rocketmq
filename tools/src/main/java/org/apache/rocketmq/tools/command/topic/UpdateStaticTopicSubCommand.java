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

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicRemappingDetailWrapper;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminUtils;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateStaticTopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateStaticTopic";
    }

    @Override
    public String commandDesc() {
        return "Update or create static topic, which has fixed number of queues";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = null;

        opt = new Option("c", "clusters", true, "remapping static topic to clusters, comma separated");
        optionGroup.addOption(opt);

        opt = new Option("b", "brokers", true, "remapping static topic to brokers, comma separated");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("qn", "totalQueueNum", true, "total queue num");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("mf", "mapFile", true, "The mapping data file name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("fr", "forceReplace", true, "Force replace the old mapping");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }



    public void executeFromFile(final CommandLine commandLine, final Options options,
                        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue('t').trim();
            String mapFileName = commandLine.getOptionValue('f').trim();
            String mapData = MixAll.file2String(mapFileName);
            TopicRemappingDetailWrapper wrapper = TopicRemappingDetailWrapper.decode(mapData.getBytes(StandardCharsets.UTF_8),
                TopicRemappingDetailWrapper.class);
            //double check the config
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, wrapper.getBrokerConfigMap());
            boolean force = false;
            if (commandLine.hasOption("fr") && Boolean.parseBoolean(commandLine.getOptionValue("fr").trim())) {
                force = true;
            }
            TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(wrapper.getBrokerConfigMap().values())), false, true);

            MQAdminUtils.completeNoTargetBrokers(wrapper.getBrokerConfigMap(), defaultMQAdminExt);
            MQAdminUtils.updateTopicConfigMappingAll(wrapper.getBrokerConfigMap(), defaultMQAdminExt, false);
            return;
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }



    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        if (!commandLine.hasOption('t')) {
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
            return;
        }

        if (commandLine.hasOption("f")) {
            executeFromFile(commandLine, options, rpcHook);
            return;
        }

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        Set<String> targetBrokers = new HashSet<>();

        try {
            defaultMQAdminExt.start();
            if (!commandLine.hasOption("b") && !commandLine.hasOption('c')
                    || !commandLine.hasOption("qn")) {
                ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
                return;
            }
            String topic = commandLine.getOptionValue('t').trim();

            ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo == null
                    || clusterInfo.getClusterAddrTable().isEmpty()) {
                throw new RuntimeException("The Cluster info is empty");
            }
            {
                if (commandLine.hasOption("b")) {
                    String brokerStrs = commandLine.getOptionValue("b").trim();
                    for (String broker: brokerStrs.split(",")) {
                        targetBrokers.add(broker.trim());
                    }
                } else if (commandLine.hasOption("c")) {
                    String clusters = commandLine.getOptionValue('c').trim();
                    for (String cluster : clusters.split(",")) {
                        cluster = cluster.trim();
                        if (clusterInfo.getClusterAddrTable().get(cluster) != null) {
                            targetBrokers.addAll(clusterInfo.getClusterAddrTable().get(cluster));
                        }
                    }
                }
                if (targetBrokers.isEmpty()) {
                    throw new RuntimeException("Find none brokers, do nothing");
                }
            }

            //get the existed topic config and mapping

            brokerConfigMap = MQAdminUtils.examineTopicConfigAll(topic, defaultMQAdminExt);
            int queueNum = Integer.parseInt(commandLine.getOptionValue("qn").trim());

            Map.Entry<Long, Integer> maxEpochAndNum = new AbstractMap.SimpleImmutableEntry<>(System.currentTimeMillis(), queueNum);
            if (!brokerConfigMap.isEmpty()) {
                maxEpochAndNum = TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            }

            {
                TopicRemappingDetailWrapper oldWrapper = new TopicRemappingDetailWrapper(topic, TopicRemappingDetailWrapper.TYPE_CREATE_OR_UPDATE, maxEpochAndNum.getKey(), brokerConfigMap, new HashSet<>(), new HashSet<>());
                String oldMappingDataFile = TopicQueueMappingUtils.writeToTemp(oldWrapper, false);
                System.out.printf("The old mapping data is written to file " + oldMappingDataFile + "\n");
            }
            //add the existed brokers to target brokers
            targetBrokers.addAll(brokerConfigMap.keySet());

            //calculate the new data
            TopicRemappingDetailWrapper newWrapper = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, brokerConfigMap);

            {
                String newMappingDataFile = TopicQueueMappingUtils.writeToTemp(newWrapper, true);
                System.out.printf("The new mapping data is written to file " + newMappingDataFile + "\n");
            }

            MQAdminUtils.completeNoTargetBrokers(brokerConfigMap, defaultMQAdminExt);
            MQAdminUtils.updateTopicConfigMappingAll(brokerConfigMap, defaultMQAdminExt, false);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
