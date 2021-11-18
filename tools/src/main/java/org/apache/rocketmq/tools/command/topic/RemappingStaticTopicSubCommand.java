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

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.LogicQueueMappingItem;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.TopicQueueMappingDetail;
import org.apache.rocketmq.common.TopicQueueMappingOne;
import org.apache.rocketmq.common.TopicQueueMappingUtils;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class RemappingStaticTopicSubCommand implements SubCommand {

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
        return options;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        ClientMetadata clientMetadata = new ClientMetadata();
        Map<String, TopicConfigAndQueueMapping> existedTopicConfigMap = new HashMap<>();
        Map<Integer, TopicQueueMappingOne> globalIdMap = new HashMap<>();
        Set<String> brokers = new HashSet<>();
        Map.Entry<Long, Integer> maxEpochAndNum = null;
        try {
            if ((!commandLine.hasOption("b") && !commandLine.hasOption('c'))
                    || !commandLine.hasOption('t')) {
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

            if (commandLine.hasOption("b")) {
                String brokerStrs = commandLine.getOptionValue("b").trim();
                for (String broker: brokerStrs.split(",")) {
                    brokers.add(broker.trim());
                }
            } else if (commandLine.hasOption("c")) {
                String clusters = commandLine.getOptionValue('c').trim();
                for (String cluster : clusters.split(",")) {
                    cluster = cluster.trim();
                    if (clusterInfo.getClusterAddrTable().get(cluster) != null) {
                        brokers.addAll(clusterInfo.getClusterAddrTable().get(cluster));
                    }
                }
            }
            if (brokers.isEmpty()) {
                throw new RuntimeException("Find none brokers");
            }
            for (String broker : brokers) {
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                if (addr == null) {
                    throw new RuntimeException("Can't find addr for broker " + broker);
                }
            }

            //get the existed topic config and mapping
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
                        existedTopicConfigMap.put(bname, mapping);
                    }
                }
            }

            if (existedTopicConfigMap.isEmpty()) {
                throw new RuntimeException("No topic route to do the remapping");
            }

            //make sure it it not null
            existedTopicConfigMap.forEach((key, value) -> {
                if (value.getMappingDetail() != null) {
                    throw new RuntimeException("Mapping info should be null in broker " + key);
                }
            });
            //make sure the detail is not dirty
            existedTopicConfigMap.forEach((key, value) -> {
                if (!key.equals(value.getMappingDetail().getBname())) {
                    throw new RuntimeException(String.format("The broker name is not equal %s != %s ", key, value.getMappingDetail().getBname()));
                }
                if (value.getMappingDetail().isDirty()) {
                    throw new RuntimeException("The mapping info is dirty in broker  " + value.getMappingDetail().getBname());
                }
            });

            List<TopicQueueMappingDetail> detailList = existedTopicConfigMap.values().stream().map(TopicConfigAndQueueMapping::getMappingDetail).collect(Collectors.toList());
            //check the epoch and qnum
            maxEpochAndNum = TopicQueueMappingUtils.findMaxEpochAndQueueNum(detailList);
            final Map.Entry<Long, Integer> tmpMaxEpochAndNum = maxEpochAndNum;
            detailList.forEach( mappingDetail -> {
                if (tmpMaxEpochAndNum.getKey() != mappingDetail.getEpoch()) {
                    throw new RuntimeException(String.format("epoch dose not match %d != %d in %s", tmpMaxEpochAndNum.getKey(), mappingDetail.getEpoch(), mappingDetail.getBname()));
                }
                if (tmpMaxEpochAndNum.getValue() != mappingDetail.getTotalQueues()) {
                    throw new RuntimeException(String.format("total queue number dose not match %d != %d in %s", tmpMaxEpochAndNum.getValue(), mappingDetail.getTotalQueues(), mappingDetail.getBname()));
                }
            });

            globalIdMap = TopicQueueMappingUtils.buildMappingItems(new ArrayList<>(detailList), false);

            if (maxEpochAndNum.getValue() != globalIdMap.size()) {
                throw new RuntimeException(String.format("The total queue number in config dose not match the real hosted queues %d != %d", maxEpochAndNum.getValue(), globalIdMap.size()));
            }
            for (int i = 0; i < maxEpochAndNum.getValue(); i++) {
                if (!globalIdMap.containsKey(i)) {
                    throw new RuntimeException(String.format("The queue number %s is not in globalIdMap", i));
                }
            }

            //the check is ok, now do the mapping allocation
            int maxNum = maxEpochAndNum.getValue();
            long maxEpoch = maxEpochAndNum.getKey();
            TopicQueueMappingUtils.MappingAllocator allocator = TopicQueueMappingUtils.buildMappingAllocator(new HashMap<>(), brokers.stream().collect(Collectors.toMap( x -> x, x -> 0)));
            allocator.upToNum(maxNum);
            Map<String, Integer> expectedBrokerNumMap = allocator.getBrokerNumMap();
            Queue<Integer> waitAssignQueues = new ArrayDeque<Integer>();
            Map<Integer, String> expectedIdToBroker = new HashMap<>();
            //the following logic will make sure that, for one broker, either "map in" or "map out"
            //It can't both,  map in some queues but also map out some queues.
            globalIdMap.forEach((queueId, mappingOne) -> {
                String leaderBroker = mappingOne.getBname();
                if (expectedBrokerNumMap.containsKey(leaderBroker)) {
                    if (expectedBrokerNumMap.get(leaderBroker) > 0) {
                        expectedIdToBroker.put(queueId, leaderBroker);
                        expectedBrokerNumMap.put(leaderBroker, expectedBrokerNumMap.get(leaderBroker) - 1);
                    } else {
                        waitAssignQueues.add(queueId);
                        expectedBrokerNumMap.remove(leaderBroker);
                    }
                } else {
                    waitAssignQueues.add(queueId);
                }
            });
            expectedBrokerNumMap.forEach((broker, queueNum) -> {
                for (int i = 0; i < queueNum; i++) {
                    Integer queueId = waitAssignQueues.poll();
                    assert queueId != null;
                    expectedIdToBroker.put(queueId, broker);
                }
            });

            //Now construct the remapping info
            Set<String> brokersToMapOut = new HashSet<>();
            Set<String> brokersToMapIn = new HashSet<>();
            for (Map.Entry<Integer, String> mapEntry : expectedIdToBroker.entrySet()) {
                Integer queueId = mapEntry.getKey();
                String broker = mapEntry.getValue();
                TopicQueueMappingOne topicQueueMappingOne = globalIdMap.get(queueId);
                assert topicQueueMappingOne != null;
                if (topicQueueMappingOne.getBname().equals(broker)) {
                    continue;
                }
                //remapping
                String mapInBroker = broker;
                String mapOutBroker = topicQueueMappingOne.getBname();
                brokersToMapIn.add(mapInBroker);
                brokersToMapOut.add(mapOutBroker);
                TopicConfigAndQueueMapping mapInConfig = existedTopicConfigMap.get(mapInBroker);
                TopicConfigAndQueueMapping mapOutConfig = existedTopicConfigMap.get(mapOutBroker);

                mapInConfig.setWriteQueueNums(mapInConfig.getWriteQueueNums() + 1);
                mapInConfig.setWriteQueueNums(mapInConfig.getWriteQueueNums() + 1);

                List<LogicQueueMappingItem> items = new ArrayList<>(topicQueueMappingOne.getItems());
                LogicQueueMappingItem last = items.get(items.size() - 1);
                items.add(new LogicQueueMappingItem(last.getGen() + 1, mapInConfig.getWriteQueueNums() - 1, mapInBroker, -1, 0, -1, -1, -1));

                ImmutableList<LogicQueueMappingItem> resultItems = ImmutableList.copyOf(items);
                //Use the same object
                mapInConfig.getMappingDetail().putMappingInfo(queueId, resultItems);
                mapOutConfig.getMappingDetail().putMappingInfo(queueId, resultItems);
            }

            long epoch = Math.max(maxEpochAndNum.getKey() + 1000, System.currentTimeMillis());
            existedTopicConfigMap.values().forEach( configMapping -> {
                configMapping.getMappingDetail().setEpoch(epoch);
                configMapping.getMappingDetail().setTotalQueues(maxNum);
            });
            // now do the remapping
            //Step1: let the new leader can be write without the logicOffset
            for (String broker: brokersToMapIn) {
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                TopicConfigAndQueueMapping configMapping = existedTopicConfigMap.get(broker);
                defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail());
            }
            //Step2: forbid the write of old leader
            for (String broker: brokersToMapOut) {
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                TopicConfigAndQueueMapping configMapping = existedTopicConfigMap.get(broker);
                defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail());
            }
            //Step3: decide the logic offset
            for (String broker: brokersToMapOut) {
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                TopicStatsTable statsTable = defaultMQAdminExt.examineTopicStats(addr, topic);
                TopicConfigAndQueueMapping mapOutConfig = existedTopicConfigMap.get(broker);
                for (Map.Entry<Integer, ImmutableList<LogicQueueMappingItem>> entry : mapOutConfig.getMappingDetail().getHostedQueues().entrySet()) {
                    ImmutableList<LogicQueueMappingItem> items = entry.getValue();
                    Integer globalId = entry.getKey();
                    if (items.size() < 2) {
                        continue;
                    }
                    LogicQueueMappingItem newLeader = items.get(items.size() - 1);
                    LogicQueueMappingItem oldLeader = items.get(items.size() - 2);
                    if (newLeader.getLogicOffset()  > 0) {
                        continue;
                    }
                    TopicOffset topicOffset = statsTable.getOffsetTable().get(new MessageQueue(topic, oldLeader.getBname(), oldLeader.getQueueId()));
                    if (topicOffset == null) {
                        throw new RuntimeException("Cannot get the max offset for old leader " + oldLeader);
                    }
                    //TODO check the max offset, will it return -1?
                    if (topicOffset.getMaxOffset() < oldLeader.getStartOffset()) {
                        throw new RuntimeException("The max offset is smaller then the start offset " + oldLeader + " " + topicOffset.getMaxOffset());
                    }
                    newLeader.setLogicOffset(oldLeader.computeStaticQueueOffset(topicOffset.getMaxOffset() + 10000));
                    TopicConfigAndQueueMapping mapInConfig = existedTopicConfigMap.get(newLeader.getBname());
                    //fresh the new leader
                    mapInConfig.getMappingDetail().putMappingInfo(globalId, items);
                }
            }
            //Step4: write to the new leader with logic offset
            for (String broker: brokersToMapIn) {
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                TopicConfigAndQueueMapping configMapping = existedTopicConfigMap.get(broker);
                defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail());
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
