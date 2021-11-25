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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.common.statictopic.TopicRemappingDetailWrapper;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        ClientMetadata clientMetadata = new ClientMetadata();

        try {
            String topic = commandLine.getOptionValue('t').trim();
            String mapFileName = commandLine.getOptionValue('f').trim();
            String mapData = MixAll.file2String(mapFileName);
            TopicRemappingDetailWrapper wrapper = TopicRemappingDetailWrapper.decode(mapData.getBytes(), TopicRemappingDetailWrapper.class);
            //double check the config
            TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, wrapper.getBrokerConfigMap());
            boolean force = false;
            if (commandLine.hasOption("fr") && Boolean.parseBoolean(commandLine.getOptionValue("fr").trim())) {
                force = true;
            }
            TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(wrapper.getBrokerConfigMap().values())), false, true);

            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo == null
                    || clusterInfo.getClusterAddrTable().isEmpty()) {
                throw new RuntimeException("The Cluster info is empty");
            }
            clientMetadata.refreshClusterInfo(clusterInfo);

            doUpdate(wrapper.getBrokerConfigMap(), clientMetadata, defaultMQAdminExt, force);
            return;
        }catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    public void doUpdate(Map<String, TopicConfigAndQueueMapping> brokerConfigMap, ClientMetadata clientMetadata, DefaultMQAdminExt defaultMQAdminExt, boolean force) throws Exception {
        //check it before
        for (String broker : brokerConfigMap.keySet()) {
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            if (addr == null) {
                throw new RuntimeException("Can't find addr for broker " + broker);
            }
        }
        //If some succeed, and others fail, it will cause inconsistent data
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            String broker = entry.getKey();
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), force);
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
        ClientMetadata clientMetadata = new ClientMetadata();

        Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();
        Set<String> targetBrokers = new HashSet<>();

        try {
            if ((!commandLine.hasOption("b") && !commandLine.hasOption('c'))
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
            clientMetadata.refreshClusterInfo(clusterInfo);
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
                for (String broker : targetBrokers) {
                    String addr = clientMetadata.findMasterBrokerAddr(broker);
                    if (addr == null) {
                        throw new RuntimeException("Can't find addr for broker " + broker);
                    }
                }
            }

            //get the existed topic config and mapping

            brokerConfigMap = defaultMQAdminExt.examineTopicConfigAll(clientMetadata, topic);
            int queueNum = Integer.parseInt(commandLine.getOptionValue("qn").trim());

            Map.Entry<Long, Integer> maxEpochAndNum = new AbstractMap.SimpleImmutableEntry<>(System.currentTimeMillis(), queueNum);
            if (!brokerConfigMap.isEmpty()) {
                maxEpochAndNum = TopicQueueMappingUtils.checkNameEpochNumConsistence(topic, brokerConfigMap);
            }

            {
                TopicRemappingDetailWrapper oldWrapper = new TopicRemappingDetailWrapper(topic, TopicRemappingDetailWrapper.TYPE_CREATE_OR_UPDATE, maxEpochAndNum.getKey(), brokerConfigMap, new HashSet<String>(), new HashSet<String>());
                String oldMappingDataFile = TopicQueueMappingUtils.writeToTemp(oldWrapper, false);
                System.out.println("The old mapping data is written to file " + oldMappingDataFile);
            }

            //calculate the new data
            TopicRemappingDetailWrapper newWrapper = TopicQueueMappingUtils.createTopicConfigMapping(topic, queueNum, targetBrokers, new HashSet<>(), brokerConfigMap);

            {
                String newMappingDataFile = TopicQueueMappingUtils.writeToTemp(newWrapper, true);
                System.out.println("The new mapping data is written to file " + newMappingDataFile);
            }

            doUpdate(newWrapper.getBrokerConfigMap(), clientMetadata, defaultMQAdminExt, false);

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
