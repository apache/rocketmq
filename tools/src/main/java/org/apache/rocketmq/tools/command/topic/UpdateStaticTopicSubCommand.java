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
import org.apache.rocketmq.common.LogicQueueMappingItem;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private void validateIfNull(Map.Entry<String, TopicConfigAndQueueMapping> entry, boolean shouldNull) {
        if (shouldNull) {
            if (entry.getValue().getTopicQueueMappingInfo() != null) {
                throw new RuntimeException("Mapping info should be null in broker " + entry.getKey());
            }
        } else {
            if (entry.getValue().getTopicQueueMappingInfo() == null) {
                throw new RuntimeException("Mapping info should not be null in broker  " + entry.getKey());
            }
        }
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

            String topic = commandLine.getOptionValue('t').trim();
            int queueNum = Integer.parseInt(commandLine.getOptionValue("qn").trim());
            String cluster = commandLine.getOptionValue('c').trim();
            ClusterInfo clusterInfo  = defaultMQAdminExt.examineBrokerClusterInfo();
            if (clusterInfo == null
                    || clusterInfo.getClusterAddrTable().isEmpty()
                    || clusterInfo.getClusterAddrTable().get(cluster) == null
                    || clusterInfo.getClusterAddrTable().get(cluster).isEmpty()) {
                throw new RuntimeException("The Cluster info is null for " + cluster);
            }
            clientMetadata.refreshClusterInfo(clusterInfo);
            //first get the existed topic config and mapping
            {
                TopicRouteData routeData = defaultMQAdminExt.examineTopicRouteInfo(topic);
                clientMetadata.freshTopicRoute(topic, routeData);
                if (routeData != null
                    && !routeData.getQueueDatas().isEmpty()) {
                    for (QueueData queueData: routeData.getQueueDatas()) {
                        String bname = queueData.getBrokerName();
                        String addr = clientMetadata.findMasterBrokerAddr(bname);
                        TopicConfigAndQueueMapping mapping = (TopicConfigAndQueueMapping) defaultMQAdminExt.examineTopicConfig(addr, topic);
                        //allow the mapping info is null
                        if (mapping != null) {
                            existedTopicConfigMap.put(bname, mapping);
                        }
                    }
                }
            }
            // the
            {
                if (!existedTopicConfigMap.isEmpty()) {
                    //make sure it it not null
                    existedTopicConfigMap.entrySet().forEach(entry -> {
                        validateIfNull(entry, false);
                    });
                    //make sure the detail is not dirty
                    existedTopicConfigMap.entrySet().forEach(entry -> {
                        if (!entry.getKey().equals(entry.getValue().getTopicQueueMappingInfo().getBname())) {
                            throw new RuntimeException(String.format("The broker name is not equal %s != %s ",  entry.getKey(), entry.getValue().getTopicQueueMappingInfo().getBname()));
                        }
                        if (entry.getValue().getTopicQueueMappingInfo().isDirty()) {
                            throw new RuntimeException("The mapping info is dirty in broker  " + entry.getValue().getTopicQueueMappingInfo().getBname());
                        }
                    });

                    List<TopicQueueMappingDetail> detailList = existedTopicConfigMap.values().stream().map(TopicConfigAndQueueMapping::getTopicQueueMappingInfo).collect(Collectors.toList());
                    //check the epoch and qnum
                    Map.Entry<Integer, Integer> maxEpochAndNum = TopicQueueMappingUtils.findMaxEpochAndQueueNum(detailList);
                    detailList.forEach( mappingDetail -> {
                        if (maxEpochAndNum.getKey() != mappingDetail.getEpoch()) {
                            throw new RuntimeException(String.format("epoch dose not match %d != %d in %s", maxEpochAndNum.getKey(), mappingDetail.getEpoch(), mappingDetail.getBname()));
                        }
                        if (maxEpochAndNum.getValue() != mappingDetail.getTotalQueues()) {
                            throw new RuntimeException(String.format("total queue number dose not match %d != %d in %s", maxEpochAndNum.getValue(), mappingDetail.getTotalQueues(), mappingDetail.getBname()));
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
            }
            if (queueNum < globalIdMap.size()) {
                throw new RuntimeException(String.format("Cannot decrease the queue num for static topic %d < %d", queueNum, globalIdMap.size()));
            }
            //check the queue number
            if (queueNum == globalIdMap.size()) {
                throw new RuntimeException("The topic queue num is equal the existed queue num, do nothing");
            }
            //the check is ok, now do the real



            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
