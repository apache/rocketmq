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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportMetaDataCommand implements SubCommand {

    @Override
    public String commandName() {
        return "exportMetaData";
    }

    @Override
    public String commandDesc() {
        return "export meta data";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true, "export metadata.json path | default /tmp/rocketmq/config");
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

            String clusterName = commandLine.getOptionValue('c').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/config" : commandLine.getOptionValue('f')
                .trim();

            Set<String> masterSet =
                CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);

            ConcurrentMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<>();
            ConcurrentMap<String, SubscriptionGroupConfig> subGroupConfigMap = new ConcurrentHashMap<>();

            for (String addr : masterSet) {
                TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getUserTopicConfig(
                    addr, 10000L);

                for (Map.Entry<String, TopicConfig> entry : topicConfigSerializeWrapper.getTopicConfigTable()
                    .entrySet()) {
                    if (!entry.getKey().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) &&
                        !entry.getKey().startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                        TopicConfig topicConfig = topicConfigMap.get(entry.getKey());
                        if (null != topicConfig) {
                            entry.getValue().setWriteQueueNums(
                                topicConfig.getWriteQueueNums() + entry.getValue().getWriteQueueNums());
                            entry.getValue().setReadQueueNums(
                                topicConfig.getReadQueueNums() + entry.getValue().getReadQueueNums());
                        }
                        topicConfigMap.put(entry.getKey(), entry.getValue());
                    }
                }

                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getUserSubscriptionGroup(
                    addr, 10000);
                for (Map.Entry<String, SubscriptionGroupConfig> entry : subscriptionGroupWrapper
                    .getSubscriptionGroupTable().entrySet()) {

                    SubscriptionGroupConfig subscriptionGroupConfig = subGroupConfigMap.get(entry.getKey());
                    if (null != subscriptionGroupConfig) {
                        entry.getValue().setRetryQueueNums(
                            subscriptionGroupConfig.getRetryQueueNums() + entry.getValue().getRetryQueueNums());
                    }
                    subGroupConfigMap.put(entry.getKey(), entry.getValue());
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("topicConfigTable", topicConfigMap);
            result.put("subscriptionGroupTable", subGroupConfigMap);
            result.put("rocketmqVersion", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
            result.put("exportTime", System.currentTimeMillis());

            String path = filePath + "/metadata.json";

            MixAll.string2FileNotSafe(JSON.toJSONString(result, true), path);

            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
