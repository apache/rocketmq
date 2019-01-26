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
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateTopicPermSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopicPerm";
    }

    @Override
    public String commandDesc() {
        return "Update topic perm";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(2|4|6), intro[2:W; 4:R; 6:RW]");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    private DefaultMQAdminExt defaultMQAdminExt;

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {

        int newPerm = Integer.parseInt(commandLine.getOptionValue('p').trim());

        if (newPerm != 2 && newPerm != 4 && newPerm != 6) {
            System.out.printf("perm only support 2(W), 4(R) or 6(RW)");
            return;
        }


        try {
            if (defaultMQAdminExt == null) {
                defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
                defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
                defaultMQAdminExt.start();
            }
            String topic = commandLine.getOptionValue('t').trim();
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            assert topicRouteData != null;
            List<QueueData> queueDatas = topicRouteData.getQueueDatas();
            assert queueDatas != null && queueDatas.size() > 0;


            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();

            HashMap<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();


            if (commandLine.hasOption('b')) {

                String addr = commandLine.getOptionValue('b').trim();
                updatePerm(queueDatas, brokerAddrTable, addr, newPerm, topic);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                Set<String> masterSet =
                        CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    updatePerm(queueDatas, brokerAddrTable, addr, newPerm, topic);

                }

            } else {
                ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }


    private QueueData getQueueData(List<QueueData> queueDatas, HashMap<String, BrokerData> brokerAddrTable, String addr) {
        for (Map.Entry<String, BrokerData> entry : brokerAddrTable.entrySet()) {
            String name = entry.getValue().getBrokerName();
            String master = entry.getValue().getBrokerAddrs().get(MixAll.MASTER_ID);
            if (!addr.equalsIgnoreCase(master)) {
                continue;
            } else {
                for (QueueData data : queueDatas) {
                    if (data.getBrokerName().equalsIgnoreCase(name)) {
                        return data;
                    }
                }

            }
        }
        return null;
    }


    private void updatePerm(List<QueueData> queueDatas, HashMap<String, BrokerData> brokerAddrTable, String addr, int newPerm, String topic) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        QueueData data = getQueueData(queueDatas, brokerAddrTable, addr);
        if (data == null) {
            System.out.printf("no master broker can be found to update", addr);
            return;
        }
        if (data.getPerm() == newPerm) {
            System.out.printf("new perm equals to the old one on %s !%n", addr);
            return;
        }


        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topic);
        topicConfig.setPerm(newPerm);
        topicConfig.setTopicSysFlag(data.getTopicSynFlag());
        topicConfig.setWriteQueueNums(data.getWriteQueueNums());
        topicConfig.setReadQueueNums(data.getReadQueueNums());

        defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
        System.out.printf("update topic perm to %s in %s success.%n", newPerm, addr);
        System.out.printf("%s%n", topicConfig);
        return;
    }

}
