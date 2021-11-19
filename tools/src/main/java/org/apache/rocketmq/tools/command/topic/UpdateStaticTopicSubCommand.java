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

import com.google.common.collect.ImmutableList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingOne;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
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
import java.util.stream.Collectors;

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

        opt = new Option("f", "mapFile", true, "The map file name");
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
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, wrapper.getBrokerConfigMap());
            TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(wrapper.getBrokerConfigMap().values())), false, true);

            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo == null
                    || clusterInfo.getClusterAddrTable().isEmpty()) {
                throw new RuntimeException("The Cluster info is empty");
            }
            clientMetadata.refreshClusterInfo(clusterInfo);
            doUpdate(wrapper.getBrokerConfigMap(), clientMetadata, defaultMQAdminExt);
            return;
        }catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    public void doUpdate(Map<String, TopicConfigAndQueueMapping> brokerConfigMap, ClientMetadata clientMetadata, DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        //If some succeed, and others fail, it will cause inconsistent data
        for (Map.Entry<String, TopicConfigAndQueueMapping> entry : brokerConfigMap.entrySet()) {
            String broker = entry.getKey();
            String addr = clientMetadata.findMasterBrokerAddr(broker);
            TopicConfigAndQueueMapping configMapping = entry.getValue();
            defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail(), false);
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
        Map<Integer, TopicQueueMappingOne> globalIdMap = new HashMap<>();
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
            int queueNum = Integer.parseInt(commandLine.getOptionValue("qn").trim());
            {
                TopicRouteData routeData = defaultMQAdminExt.examineTopicRouteInfo(topic);
                clientMetadata.freshTopicRoute(topic, routeData);

                if (routeData != null
                        && !routeData.getQueueDatas().isEmpty()) {
                    for (QueueData queueData: routeData.getQueueDatas()) {
                        String bname = queueData.getBrokerName();
                        String addr = clientMetadata.findMasterBrokerAddr(bname);
                        TopicConfigAndQueueMapping mapping = (TopicConfigAndQueueMapping) defaultMQAdminExt.examineTopicConfig(addr, topic);
                        //allow the config is null
                        if (mapping != null) {
                            brokerConfigMap.put(bname, mapping);
                        }
                    }
                }
            }

            Map.Entry<Long, Integer> maxEpochAndNum = new AbstractMap.SimpleImmutableEntry<>(System.currentTimeMillis(), queueNum);
            if (!brokerConfigMap.isEmpty()) {
                maxEpochAndNum = TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
                globalIdMap = TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);
            }
            if (queueNum < globalIdMap.size()) {
                throw new RuntimeException(String.format("Cannot decrease the queue num for static topic %d < %d", queueNum, globalIdMap.size()));
            }
            //check the queue number
            if (queueNum == globalIdMap.size()) {
                throw new RuntimeException("The topic queue num is equal the existed queue num, do nothing");
            }

            {
                TopicRemappingDetailWrapper oldWrapper = new TopicRemappingDetailWrapper(topic, TopicRemappingDetailWrapper.TYPE_CREATE_OR_UPDATE, maxEpochAndNum.getKey(), new HashMap<>(), brokerConfigMap);
                String oldMappingDataFile = TopicQueueMappingUtils.writeToTemp(oldWrapper, false);
                System.out.println("The old mapping data is written to file " + oldMappingDataFile);
            }


            //the check is ok, now do the mapping allocation
            Map<String, Integer> brokerNumMap = targetBrokers.stream().collect(Collectors.toMap( x -> x, x -> 0));
            final Map<Integer, String> oldIdToBroker = new HashMap<>();
            globalIdMap.forEach((key, value) -> {
                String leaderbroker = value.getBname();
                oldIdToBroker.put(key, leaderbroker);
                if (!brokerNumMap.containsKey(leaderbroker)) {
                    brokerNumMap.put(leaderbroker, 1);
                } else {
                    brokerNumMap.put(leaderbroker, brokerNumMap.get(leaderbroker) + 1);
                }
            });
            TopicQueueMappingUtils.MappingAllocator allocator = TopicQueueMappingUtils.buildMappingAllocator(oldIdToBroker, brokerNumMap);
            allocator.upToNum(queueNum);
            Map<Integer, String> newIdToBroker = allocator.getIdToBroker();

            //construct the topic configAndMapping
            long newEpoch = Math.max(maxEpochAndNum.getKey() + 1000, System.currentTimeMillis());
            for (Map.Entry<Integer, String> e : newIdToBroker.entrySet()) {
                Integer queueId = e.getKey();
                String broker = e.getValue();
                if (globalIdMap.containsKey(queueId)) {
                    //ignore the exited
                    continue;
                }
                TopicConfigAndQueueMapping configMapping;
                if (!brokerConfigMap.containsKey(broker)) {
                    configMapping = new TopicConfigAndQueueMapping(new TopicConfig(topic), new TopicQueueMappingDetail(topic, 0, broker, -1));
                    configMapping.setWriteQueueNums(1);
                    configMapping.setReadQueueNums(1);
                    brokerConfigMap.put(broker, configMapping);
                } else {
                    configMapping = brokerConfigMap.get(broker);
                    configMapping.setWriteQueueNums(configMapping.getWriteQueueNums() + 1);
                    configMapping.setReadQueueNums(configMapping.getReadQueueNums() + 1);
                }
                LogicQueueMappingItem mappingItem = new LogicQueueMappingItem(0, configMapping.getWriteQueueNums() - 1, broker, 0, 0, -1, -1, -1);
                configMapping.getMappingDetail().putMappingInfo(queueId, ImmutableList.of(mappingItem));
            }

            // set the topic config
            brokerConfigMap.values().forEach(configMapping -> {
                configMapping.getMappingDetail().setEpoch(newEpoch);
                configMapping.getMappingDetail().setTotalQueues(queueNum);
            });
            //double check the config
            TopicQueueMappingUtils.checkConsistenceOfTopicConfigAndQueueMapping(topic, brokerConfigMap);
            TopicQueueMappingUtils.checkAndBuildMappingItems(new ArrayList<>(TopicQueueMappingUtils.getMappingDetailFromConfig(brokerConfigMap.values())), false, true);

            {
                TopicRemappingDetailWrapper newWrapper = new TopicRemappingDetailWrapper(topic, TopicRemappingDetailWrapper.TYPE_CREATE_OR_UPDATE, newEpoch, newIdToBroker, brokerConfigMap);
                String newMappingDataFile = TopicQueueMappingUtils.writeToTemp(newWrapper, true);
                System.out.println("The new mapping data is written to file " + newMappingDataFile);
            }

            doUpdate(brokerConfigMap, clientMetadata, defaultMQAdminExt);

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
