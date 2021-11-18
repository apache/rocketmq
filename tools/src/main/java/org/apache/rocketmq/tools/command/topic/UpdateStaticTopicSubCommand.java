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
import com.sun.xml.internal.ws.api.BindingIDFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.LogicQueueMappingItem;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.TopicQueueMappingDetail;
import org.apache.rocketmq.common.TopicQueueMappingUtils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("qn", "totalQueueNum", true, "total queue num");
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
        Map<Integer, ImmutableList<LogicQueueMappingItem>> globalIdMap = new HashMap<>();
        try {
            if (!commandLine.hasOption('t')
                    || !commandLine.hasOption('c')
                    || !commandLine.hasOption("qn")) {
                ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
                return;
            }
            String topic = commandLine.getOptionValue('t').trim();
            int queueNum = Integer.parseInt(commandLine.getOptionValue("qn").trim());
            String clusters = commandLine.getOptionValue('c').trim();
            ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo == null
                    || clusterInfo.getClusterAddrTable().isEmpty()) {
                throw new RuntimeException("The Cluster info is empty");
            } else {
                clientMetadata.refreshClusterInfo(clusterInfo);
            }
            Set<String> brokers = new HashSet<>();
            for (String cluster : clusters.split(",")) {
                cluster = cluster.trim();
                if (clusterInfo.getClusterAddrTable().get(cluster) != null) {
                    brokers.addAll(clusterInfo.getClusterAddrTable().get(cluster));
                }
            }
            if (brokers.isEmpty()) {
                throw new RuntimeException("Find none brokers for " + clusters);
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

            Map.Entry<Long, Integer> maxEpochAndNum = new AbstractMap.SimpleImmutableEntry<>(System.currentTimeMillis(), queueNum);
            if (!existedTopicConfigMap.isEmpty()) {
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
            }
            if (queueNum < globalIdMap.size()) {
                throw new RuntimeException(String.format("Cannot decrease the queue num for static topic %d < %d", queueNum, globalIdMap.size()));
            }
            //check the queue number
            if (queueNum == globalIdMap.size()) {
                throw new RuntimeException("The topic queue num is equal the existed queue num, do nothing");
            }
            //the check is ok, now do the mapping allocation
            Map<String, Integer> brokerNumMap = brokers.stream().collect(Collectors.toMap( x -> x, x -> 0));
            Map<Integer, String> idToBroker = new HashMap<>();
            globalIdMap.forEach((key, value) -> {
                String leaderbroker = TopicQueueMappingUtils.getLeaderBroker(value);
                idToBroker.put(key, leaderbroker);
                if (!brokerNumMap.containsKey(leaderbroker)) {
                    brokerNumMap.put(leaderbroker, 1);
                } else {
                    brokerNumMap.put(leaderbroker, brokerNumMap.get(leaderbroker) + 1);
                }
            });
            TopicQueueMappingUtils.MappingAllocator allocator = TopicQueueMappingUtils.buildMappingAllocator(idToBroker, brokerNumMap);
            allocator.upToNum(queueNum);
            Map<Integer, String> newIdToBroker = allocator.getIdToBroker();

            //construct the topic configAndMapping
            long epoch = Math.max(maxEpochAndNum.getKey() + 1000, System.currentTimeMillis());
            for (Map.Entry<Integer, String> e : newIdToBroker.entrySet()) {
                Integer queueId = e.getKey();
                String value = e.getValue();
                if (globalIdMap.containsKey(queueId)) {
                    //ignore the exited
                    continue;
                }
                TopicConfigAndQueueMapping configMapping;
                if (!existedTopicConfigMap.containsKey(value)) {
                    TopicConfig topicConfig = new TopicConfig(topic, 1, 1);
                    TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(topic, queueNum, value, epoch);
                    configMapping = new TopicConfigAndQueueMapping(topicConfig, mappingDetail);
                } else {
                    configMapping = existedTopicConfigMap.get(value);
                    configMapping.setWriteQueueNums(configMapping.getWriteQueueNums() + 1);
                    configMapping.setWriteQueueNums(configMapping.getWriteQueueNums() + 1);
                    configMapping.getMappingDetail().setEpoch(epoch);
                    configMapping.getMappingDetail().setTotalQueues(queueNum);
                }
                LogicQueueMappingItem mappingItem = new LogicQueueMappingItem(0, configMapping.getWriteQueueNums() - 1, value, 0, 0, -1, -1, -1);
                configMapping.getMappingDetail().putMappingInfo(queueId, ImmutableList.of(mappingItem));
            }

            //If some succeed, and others fail, it will cause inconsistent data
            for (Map.Entry<String, TopicConfigAndQueueMapping> entry : existedTopicConfigMap.entrySet()) {
                String broker = entry.getKey();
                String addr = clientMetadata.findMasterBrokerAddr(broker);
                TopicConfigAndQueueMapping configMapping = entry.getValue();
                defaultMQAdminExt.createStaticTopic(addr, defaultMQAdminExt.getCreateTopicKey(), configMapping, configMapping.getMappingDetail());
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
