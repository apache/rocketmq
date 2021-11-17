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
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.rpc.ClientMetadata;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

    private void validate(Map.Entry<String, TopicConfigAndQueueMapping> entry, boolean shouldNull) {
        if (shouldNull) {
            if (entry.getValue().getTopicQueueMappingInfo() != null) {
                throw new RuntimeException("Mapping info should be null in broker " + entry.getKey());
            }
        } else {
            if (entry.getValue().getTopicQueueMappingInfo() == null) {
                throw new RuntimeException("Mapping info should not be null in broker  " + entry.getKey());
            }
            if (!entry.getKey().equals(entry.getValue().getTopicQueueMappingInfo().getBname())) {
                throw new RuntimeException(String.format("The broker name is not equal %s != %s ",  entry.getKey(), entry.getValue().getTopicQueueMappingInfo().getBname()));
            }
        }
    }

    public void validateQueueMappingInfo(Map<Integer, ImmutableList<LogicQueueMappingItem>> globalIdMap, TopicQueueMappingDetail mappingDetail) {
        if (mappingDetail.isDirty()) {
            throw new RuntimeException("The mapping info is dirty in broker  " + mappingDetail.getBname());
        }
        for (Map.Entry<Integer, ImmutableList<LogicQueueMappingItem>>  entry : mappingDetail.getHostedQueues().entrySet()) {
            Integer globalid = entry.getKey();
            String leaerBrokerName  = entry.getValue().iterator().next().getBname();
            if (!leaerBrokerName.equals(mappingDetail.getBname())) {
                //not the leader
                continue;
            }
            if (globalIdMap.containsKey(globalid)) {
                throw new RuntimeException(String.format("The queue id is duplicated in broker %s %s", leaerBrokerName, mappingDetail.getBname()));
            } else {
                globalIdMap.put(globalid, entry.getValue());
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
            {
                if (!existedTopicConfigMap.isEmpty()) {
                    Iterator<Map.Entry<String, TopicConfigAndQueueMapping>> it = existedTopicConfigMap.entrySet().iterator();
                    Map.Entry<String, TopicConfigAndQueueMapping> first = it.next();
                    validate(first, false);
                    validateQueueMappingInfo(globalIdMap, first.getValue().getTopicQueueMappingInfo());
                    TopicQueueMappingDetail firstMapping = first.getValue().getTopicQueueMappingInfo();
                    while (it.hasNext()) {
                        Map.Entry<String, TopicConfigAndQueueMapping> next = it.next();
                        validate(next, false);
                        validateQueueMappingInfo(globalIdMap, next.getValue().getTopicQueueMappingInfo());
                        TopicQueueMappingDetail nextMapping = next.getValue().getTopicQueueMappingInfo();
                        if (firstMapping.getEpoch() !=  nextMapping.getEpoch()) {
                            throw new RuntimeException(String.format("epoch dose not match %d != %d in %s %s", firstMapping.getEpoch(), nextMapping.getEpoch(), firstMapping.getBname(), nextMapping.getBname()));
                        }
                        if (firstMapping.getTotalQueues() !=  nextMapping.getTotalQueues()) {
                            throw new RuntimeException(String.format("total queue number dose not match %d != %d in %s %s", firstMapping.getTotalQueues(), nextMapping.getTotalQueues(), firstMapping.getBname(), nextMapping.getBname()));
                        }
                    }
                    if (firstMapping.getTotalQueues() != globalIdMap.size()) {
                        throw new RuntimeException(String.format("The total queue number in config dose not match the real hosted queues %d != %d", firstMapping.getTotalQueues(), globalIdMap.size()));
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
