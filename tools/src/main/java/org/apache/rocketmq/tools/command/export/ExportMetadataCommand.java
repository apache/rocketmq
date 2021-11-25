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
package org.apache.rocketmq.tools.command.export;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportMetadataCommand implements SubCommand {

    private static final String DEFAULT_FILE_PATH = "/tmp/rocketmq/export";

    @Override
    public String commandName() {
        return "exportMetadata";
    }

    @Override
    public String commandDesc() {
        return "export metadata";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "choose a broker to export");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "filePath", true, "export metadata.json path | default /tmp/rocketmq/export");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", false, "only export topic metadata");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "subscriptionGroup", false, "only export subscriptionGroup metadata");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "specialTopic", false, "need retryTopic and dlqTopic");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String filePath = !commandLine.hasOption('f') ? DEFAULT_FILE_PATH : commandLine.getOptionValue('f')
                .trim();

            boolean specialTopic = commandLine.hasOption('s');

            if (commandLine.hasOption('b')) {
                final String brokerAddr = commandLine.getOptionValue('b').trim();

                if (commandLine.hasOption('t')) {
                    filePath = filePath + "/topic.json";
                    TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getUserTopicConfig(
                        brokerAddr, specialTopic, 10000L);
                    MixAll.string2FileNotSafe(JSON.toJSONString(topicConfigSerializeWrapper, true), filePath);
                    System.out.printf("export %s success", filePath);
                } else if (commandLine.hasOption('g')) {
                    filePath = filePath + "/subscriptionGroup.json";
                    SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getUserSubscriptionGroup(
                        brokerAddr, 10000L);
                    MixAll.string2FileNotSafe(JSON.toJSONString(subscriptionGroupWrapper, true), filePath);
                    System.out.printf("export %s success", filePath);
                }
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);

                Map<String, TopicConfig> topicConfigMap = new HashMap<>();
                Map<String, SubscriptionGroupConfig> subGroupConfigMap = new HashMap<>();

                for (String addr : masterSet) {
                    TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getUserTopicConfig(
                        addr, specialTopic, 10000L);

                    SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getUserSubscriptionGroup(
                        addr, 10000);

                    if (commandLine.hasOption('t')) {
                        filePath = filePath + "/topic.json";
                        MixAll.string2FileNotSafe(JSON.toJSONString(topicConfigSerializeWrapper, true), filePath);
                        System.out.printf("export %s success", filePath);
                        return;
                    } else if (commandLine.hasOption('g')) {
                        filePath = filePath + "/subscriptionGroup.json";
                        MixAll.string2FileNotSafe(JSON.toJSONString(subscriptionGroupWrapper, true), filePath);
                        System.out.printf("export %s success", filePath);
                        return;
                    } else {
                        for (Map.Entry<String, TopicConfig> entry : topicConfigSerializeWrapper.getTopicConfigTable().entrySet()) {
                            TopicConfig topicConfig = topicConfigMap.get(entry.getKey());
                            if (null != topicConfig) {
                                entry.getValue().setWriteQueueNums(
                                    topicConfig.getWriteQueueNums() + entry.getValue().getWriteQueueNums());
                                entry.getValue().setReadQueueNums(
                                    topicConfig.getReadQueueNums() + entry.getValue().getReadQueueNums());
                            }
                            topicConfigMap.put(entry.getKey(), entry.getValue());
                        }

                        for (Map.Entry<String, SubscriptionGroupConfig> entry : subscriptionGroupWrapper.getSubscriptionGroupTable().entrySet()) {

                            SubscriptionGroupConfig subscriptionGroupConfig = subGroupConfigMap.get(entry.getKey());
                            if (null != subscriptionGroupConfig) {
                                entry.getValue().setRetryQueueNums(
                                    subscriptionGroupConfig.getRetryQueueNums() + entry.getValue().getRetryQueueNums());
                            }
                            subGroupConfigMap.put(entry.getKey(), entry.getValue());
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicConfigTable", topicConfigMap);
                        result.put("subscriptionGroupTable", subGroupConfigMap);
                        result.put("rocketmqVersion", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
                        result.put("exportTime", System.currentTimeMillis());

                        filePath = filePath + "/metadata.json";
                        MixAll.string2FileNotSafe(JSON.toJSONString(result, true), filePath);
                        System.out.printf("export %s success", filePath);
                    }

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
}

