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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.MQAdminStartup;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ImportTopicJsonCommand implements SubCommand {
    @Override
    public String commandName() {
        return "importTopicJson";
    }

    @Override
    public String commandDesc() {
        return "import topic.json to broker or cluster";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "import topic.json or subscription.json to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "import topic.json or subscription.json to which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "filePath", true, "import topic.json or subscription.json path");
        opt.setRequired(true);
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
            final String filePath = commandLine.getOptionValue('f').trim();

            String jsonString = MixAll.file2String(filePath);
            if (commandLine.hasOption('b')) {
                final String brokerAddr = commandLine.getOptionValue('b').trim();
                importTobroker(defaultMQAdminExt, brokerAddr, jsonString);
                System.out.printf("import %s to broker[%s] success", filePath, brokerAddr);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    importTobroker(defaultMQAdminExt, addr, jsonString);
                }
                System.out.printf("import %s to cluster[%s] success", filePath, clusterName);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void importTobroker(DefaultMQAdminExt defaultMQAdminExt, String brokerAddr, String jsonString)
        throws Exception {
        StringBuilder unImportTopic = new StringBuilder("");
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getAllTopicConfig(
            brokerAddr, 5000);
        Map<String, TopicConfig> topicConfigMap = topicConfigSerializeWrapper.getTopicConfigTable();
        Set<String> topicSet = topicConfigMap.keySet();

        TopicConfigSerializeWrapper importTopicConfigSerializeWrapper =
            TopicConfigSerializeWrapper.fromJson(jsonString, TopicConfigSerializeWrapper.class);

        Map<String, TopicConfig> importTopicConfigMap = importTopicConfigSerializeWrapper
            .getTopicConfigTable();
        String splitor = "";
        List<TopicConfig> topicConfigList = new LinkedList<>();
        for (Map.Entry<String, TopicConfig> entry : importTopicConfigMap.entrySet()) {
            if (TopicValidator.validateTopic(entry.getKey()) && !TopicValidator.isSystemTopic(entry.getKey())
                && !topicSet.contains(entry.getKey())) {
                topicConfigList.add(entry.getValue());
            } else {
                unImportTopic.append(splitor).append(entry.getKey());
                splitor = ";";
            }
        }
        defaultMQAdminExt.batchCreateAndUpdateTopicConfig(brokerAddr, topicConfigList);

        System.out.printf(
            "%s import failed,maybe these topic are not compliant or are system topics or already exist%n",
            unImportTopic);
    }

    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        MQAdminStartup.main(
            new String[] {new ImportTopicJsonCommand().commandName(), "-c", "DefaultCluster", "-f",
                "D:\\topic.json"});

    }
}
