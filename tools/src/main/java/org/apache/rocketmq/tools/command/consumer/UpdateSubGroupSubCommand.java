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
package org.apache.rocketmq.tools.command.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateSubGroupSubCommand implements SubCommand {

    private DefaultMQAdminExt defaultMQAdminExt;

    @Override
    public String commandName() {
        return "updateSubGroup";
    }

    @Override
    public String commandDesc() {
        return "Update or create subscription group";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create subscription group to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "create subscription group to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "consumeEnable", true, "consume enable");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "consumeFromMinEnable", true, "from min offset");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "consumeBroadcastEnable", true, "broadcast");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("q", "retryQueueNums", true, "retry queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "retryMaxTimes", true, "retry max times");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "brokerId", true, "consumer from which broker id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "whichBrokerWhenConsumeSlowly", true, "which broker id when consume slowly");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "notifyConsumerIdsChanged", true, "notify consumerId changed");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    private void updateRetryTopicQueueNums(DefaultMQAdminExt defaultMQAdminExt, String topic, String addr, Map<String, BrokerData> brokerAddrMap, Map<String, QueueData> queueDataMap, int retryQueueNums) {
        BrokerData brokerData = brokerAddrMap.get(addr);
        if (brokerAddrMap == null) {
            return;
        }

        QueueData queueData = queueDataMap.get(brokerData.getBrokerName());
        if (queueData == null) {
            return;
        }

        if (retryQueueNums != queueData.getWriteQueueNums()) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setTopicName(topic);
            topicConfig.setPerm(queueData.getPerm());
            topicConfig.setWriteQueueNums(retryQueueNums);
            topicConfig.setReadQueueNums(retryQueueNums);
            try {
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
            } catch (Exception e) {
                System.out.print("update subscription retry topic " + topic + " queue nums to " + addr + " failed\r\n");
            }
        }
    }

    private DefaultMQAdminExt getMQAdminExt(RPCHook rpcHook) {
        if (this.defaultMQAdminExt != null) {
            return defaultMQAdminExt;
        }
        defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        return defaultMQAdminExt;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
                        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = getMQAdminExt(rpcHook);

        try {
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            int defaultRetryQueueNums = subscriptionGroupConfig.getRetryQueueNums();
            subscriptionGroupConfig.setConsumeBroadcastEnable(false);
            subscriptionGroupConfig.setConsumeFromMinEnable(false);

            // groupName
            subscriptionGroupConfig.setGroupName(commandLine.getOptionValue('g').trim());

            // consumeEnable
            if (commandLine.hasOption('s')) {
                subscriptionGroupConfig.setConsumeEnable(Boolean.parseBoolean(commandLine.getOptionValue('s')
                    .trim()));
            }

            // consumeFromMinEnable
            if (commandLine.hasOption('m')) {
                subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.parseBoolean(commandLine
                    .getOptionValue('m').trim()));
            }

            // consumeBroadcastEnable
            if (commandLine.hasOption('d')) {
                subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.parseBoolean(commandLine
                    .getOptionValue('d').trim()));
            }

            // retryQueueNums
            if (commandLine.hasOption('q')) {
                subscriptionGroupConfig.setRetryQueueNums(Integer.parseInt(commandLine.getOptionValue('q')
                    .trim()));
            }

            // retryMaxTimes
            if (commandLine.hasOption('r')) {
                subscriptionGroupConfig.setRetryMaxTimes(Integer.parseInt(commandLine.getOptionValue('r')
                    .trim()));
            }

            // brokerId
            if (commandLine.hasOption('i')) {
                subscriptionGroupConfig.setBrokerId(Long.parseLong(commandLine.getOptionValue('i').trim()));
            }

            // whichBrokerWhenConsumeSlowly
            if (commandLine.hasOption('w')) {
                subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(Long.parseLong(commandLine
                    .getOptionValue('w').trim()));
            }

            // notifyConsumerIdsChanged
            if (commandLine.hasOption('a')) {
                subscriptionGroupConfig.setNotifyConsumerIdsChangedEnable(Boolean.parseBoolean(commandLine
                    .getOptionValue('a').trim()));
            }


            boolean needCheckAndUpdate = false;
            String topic = MixAll.getRetryTopic(subscriptionGroupConfig.getGroupName());
            HashMap<String, BrokerData> brokerAddrMap = new HashMap<>();
            HashMap<String, QueueData> queueDataMap = new HashMap<>();
            int retryQueueNums = subscriptionGroupConfig.getRetryQueueNums();
            if (retryQueueNums != defaultRetryQueueNums) {
                needCheckAndUpdate = true;
                TopicRouteData topicRouteData = null;
                try {
                    topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
                } catch (Exception e) {
                    System.out.print("get subscription retry topic route info null " + e.getClass() + ":" + e.getMessage() + "\r\n");
                }
                if (topicRouteData != null) {
                    List<BrokerData> brokerDatas = topicRouteData.getBrokerDatas();
                    for (BrokerData brokerData : brokerDatas) {
                        brokerAddrMap.put(brokerData.getBrokerAddrs().get(MixAll.MASTER_ID), brokerData);
                    }

                    List<QueueData> queueDatas = topicRouteData.getQueueDatas();
                    for (QueueData queueData : queueDatas) {
                        String brokerName = queueData.getBrokerName();
                        queueDataMap.put(brokerName, queueData);
                    }
                }
            }

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();

                defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);

                if (needCheckAndUpdate) {
                    updateRetryTopicQueueNums(defaultMQAdminExt, topic, addr, brokerAddrMap, queueDataMap, subscriptionGroupConfig.getRetryQueueNums());
                }

                System.out.printf("create subscription group to %s success.%n", addr);
                System.out.printf("%s", subscriptionGroupConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    try {
                        defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                        System.out.printf("create subscription group to %s success.%n", addr);

                        if (needCheckAndUpdate) {
                            updateRetryTopicQueueNums(defaultMQAdminExt, topic, addr, brokerAddrMap, queueDataMap, subscriptionGroupConfig.getRetryQueueNums());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        Thread.sleep(1000 * 1);
                    }
                }
                System.out.printf("%s", subscriptionGroupConfig);
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
