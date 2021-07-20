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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportSubscriptionCommand implements SubCommand {
    @Override
    public String commandName() {
        return "exportSubscription";
    }

    @Override
    public String commandDesc() {
        return "export subscription.csv";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true, "export subscription.csv path | default /tmp/rocketmq/config");
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
            ConcurrentMap<String, SubscriptionGroupConfig> configMap = new ConcurrentHashMap<>();

            for (String addr : masterSet) {
                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(
                    addr, 5000);
                for (Map.Entry<String, SubscriptionGroupConfig> entry : subscriptionGroupWrapper
                    .getSubscriptionGroupTable().entrySet()) {
                    if (!MixAll.isSysConsumerGroup(entry.getKey())) {
                        SubscriptionGroupConfig subscriptionGroupConfig = configMap.get(entry.getKey());
                        if (null != subscriptionGroupConfig) {
                            entry.getValue().setRetryQueueNums(
                                subscriptionGroupConfig.getRetryQueueNums() + entry.getValue().getRetryQueueNums());
                        }
                        configMap.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            StringBuilder subConfigStr = new StringBuilder(
                "groupName,consumeEnable,consumeFromMinEnable,consumeBroadcastEnable,retryQueueNums,retryMaxTimes,"
                    + "brokerId,whichBrokerWhenConsumeSlowly,notifyConsumerIdsChangedEnable\r\n");

            for (Map.Entry<String, SubscriptionGroupConfig> entry : configMap.entrySet()) {
                subConfigStr.append(entry.getValue().getGroupName());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().isConsumeEnable());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().isConsumeFromMinEnable());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().isConsumeBroadcastEnable());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().getRetryQueueNums());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().getRetryMaxTimes());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().getBrokerId());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().getWhichBrokerWhenConsumeSlowly());
                subConfigStr.append(",");
                subConfigStr.append(entry.getValue().isNotifyConsumerIdsChangedEnable());
                subConfigStr.append("\r\n");
            }

            String path = filePath + "/subscription.csv";

            write2CSV(subConfigStr.toString(), path);

            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void write2CSV(final String subConfigStr, final String path) throws IOException {
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(path);
            fileWriter.write(subConfigStr);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }
}
