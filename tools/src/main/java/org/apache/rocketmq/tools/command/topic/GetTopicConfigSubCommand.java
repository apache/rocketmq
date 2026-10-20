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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

/**
 * Prints the topic configuration stored on a broker. Unlike {@code topicRoute}, which only
 * exposes the routing view, this command returns the complete configuration persisted by the
 * broker, including attributes that affect custom message types.
 */
public class GetTopicConfigSubCommand implements SubCommand {
    private static final String FIELD_FORMAT = "%s%-40s=  %s%n";

    @Override
    public String commandName() {
        return "getTopicConfig";
    }

    @Override
    public String commandDesc() {
        return "Get topic config from a broker or cluster.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option topicOption = new Option("t", "topic", true, "topic name");
        topicOption.setRequired(true);
        options.addOption(topicOption);

        // Both values identify where to read from. Keeping them in one group prevents a user
        // from accidentally querying a broker while believing a cluster was selected.
        OptionGroup targetGroup = new OptionGroup();
        targetGroup.addOption(new Option("b", "brokerAddr", true, "get topic config from which broker"));
        targetGroup.addOption(new Option("c", "clusterName", true, "get topic config from which cluster"));
        options.addOptionGroup(targetGroup);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String topic = commandLine.getOptionValue('t').trim();
            if (commandLine.hasOption('n')) {
                defaultMQAdminExt.setNamesrvAddr(commandLine.getOptionValue('n').trim());
            }
            defaultMQAdminExt.start();

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                getAndPrint(defaultMQAdminExt, topic, brokerAddr,
                    String.format("============%s============\n", brokerAddr));
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                printClusterConfig(defaultMQAdminExt, topic, clusterName);
            } else {
                printAllBrokerConfigs(defaultMQAdminExt, topic);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void printClusterConfig(final DefaultMQAdminExt defaultMQAdminExt, final String topic,
        final String clusterName) throws Exception {
        Map<String, List<String>> masterAndSlaveMap =
            CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
        for (Entry<String, List<String>> entry : masterAndSlaveMap.entrySet()) {
            String masterAddr = entry.getKey();
            if (!CommandUtil.NO_MASTER_PLACEHOLDER.equals(masterAddr)) {
                getAndPrint(defaultMQAdminExt, topic, masterAddr,
                    String.format("============Master: %s============\n", masterAddr));
            }

            for (String slaveAddr : entry.getValue()) {
                getAndPrint(defaultMQAdminExt, topic, slaveAddr,
                    String.format("============My Master: %s=====Slave: %s============\n",
                        masterAddr, slaveAddr));
            }
        }
    }

    private void printAllBrokerConfigs(final DefaultMQAdminExt defaultMQAdminExt, final String topic)
        throws Exception {
        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
            return;
        }

        // When no explicit target is given, use the same master address selection as the other
        // mqadmin commands so a single logical broker is queried once while its slaves are not
        // mistaken for independent broker entries.
        for (Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            BrokerData brokerData = entry.getValue();
            if (brokerData == null || brokerData.getBrokerAddrs() == null
                || brokerData.getBrokerAddrs().isEmpty()) {
                continue;
            }
            String brokerAddr = brokerData.selectBrokerAddr();
            if (brokerAddr != null) {
                getAndPrint(defaultMQAdminExt, topic, brokerAddr,
                    String.format("============%s:%s============\n", entry.getKey(), brokerAddr));
            }
        }
    }

    private void getAndPrint(final DefaultMQAdminExt defaultMQAdminExt, final String topic,
        final String brokerAddr, final String printPrefix) throws InterruptedException,
        RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
        MQBrokerException {
        if (brokerAddr == null || CommandUtil.NO_MASTER_PLACEHOLDER.equals(brokerAddr)) {
            return;
        }

        TopicConfig topicConfig;
        try {
            topicConfig = defaultMQAdminExt.examineTopicConfig(brokerAddr, topic);
        } catch (MQBrokerException e) {
            // A topic may exist only on some brokers in a cluster. Treat that broker as absent
            // instead of failing the whole query and hiding the valid configurations.
            if (e.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                return;
            }
            throw e;
        }

        if (topicConfig == null) {
            return;
        }

        System.out.print(printPrefix);
        printField("topicName", topicConfig.getTopicName());
        printField("readQueueNums", topicConfig.getReadQueueNums());
        printField("writeQueueNums", topicConfig.getWriteQueueNums());
        printField("perm", String.format("%s (%d)", PermName.perm2String(topicConfig.getPerm()),
            topicConfig.getPerm()));
        printField("topicFilterType", topicConfig.getTopicFilterType());
        printField("topicSysFlag", topicConfig.getTopicSysFlag());
        printField("order", topicConfig.isOrder());
        printField("attributes", topicConfig.getAttributes());

        if (topicConfig instanceof TopicConfigAndQueueMapping) {
            printField("mappingDetail",
                ((TopicConfigAndQueueMapping) topicConfig).getMappingDetail());
        }
        System.out.printf("%n");
    }

    private void printField(final String name, final Object value) {
        System.out.printf(FIELD_FORMAT, "", name, value == null ? "" : value);
    }
}
