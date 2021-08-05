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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.MQAdminStartup;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportBrokerRuntimeInfoCommand implements SubCommand {

    @Override
    public String commandName() {
        return "exportBrokerRuntimeInfo";
    }

    @Override
    public String commandDesc() {
        return "export broker runtime info";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true,
            "export brokerRuntimeInfo.properties path | default /tmp/rocketmq/config");
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
            String clusterName = commandLine.getOptionValue('c').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/config" : commandLine.getOptionValue('f')
                .trim();

            defaultMQAdminExt.start();

            //  brokerName  brokerIp    评估报告
            Map<String, Map<String, Map<String, Map<String, Object>>>> map = new HashMap<>();

            ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
            Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                Map<String, Map<String, Map<String, Object>>> brokerMap = new HashMap();
                if (brokerData != null) {
                    Iterator<Entry<Long, String>> itAddr = brokerData.getBrokerAddrs().entrySet().iterator();
                    while (itAddr.hasNext()) {
                        Map.Entry<Long, String> next1 = itAddr.next();

                        KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());

                        Properties properties = defaultMQAdminExt.getBrokerConfig(next1.getValue());

                        TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getAllTopicConfig(
                            next1.getValue(), 10000);

                        SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(
                            next1.getValue(), 10000);

                        Map<String, Map<String, Object>> brokerInfo = new HashMap<>();

                        //broker environment,machine configuration
                        brokerInfo.put("runtimeEnv", getRuntimeEnv(kvTable, properties));

                        //broker config
                        brokerInfo.put("brokerConfig", getConfig(properties, brokerData));

                        brokerInfo.put("runtimeQuota",
                            getRuntimeQuota(kvTable, defaultMQAdminExt, next1.getValue()));

                        brokerMap.put(next1.getValue(), brokerInfo);
                    }
                }
                map.put(brokerName, brokerMap);
            }

            String path = filePath + "/brokerInfo.json";

            MixAll.string2FileNotSafe(JSON.toJSONString(map, true), path);
            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }

    }

    private Map<String, Object> getRuntimeEnv(KVTable kvTable, Properties properties) {
        Map<String, Object> runtimeEnvMap = new HashMap<>();
        runtimeEnvMap.put("cpuNum", properties.getProperty("clientCallbackExecutorThreads"));
        runtimeEnvMap.put("totalMemKBytes", kvTable.getTable().get("totalMemKBytes"));
        return runtimeEnvMap;
    }

    private Map<String, Object> getConfig(Properties properties, BrokerData brokerData) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("fileReservedTime", properties.getProperty("fileReservedTime"));
        configMap.put("flushDiskType", properties.getProperty("flushDiskType"));
        configMap.put("brokerSize", brokerData.getBrokerAddrs().size());
        return configMap;
    }

    private Map<String, Object> getRuntimeQuota(KVTable kvTable, DefaultMQAdminExt defaultMQAdminExt, String brokerAddr)
        throws Exception {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getAllTopicConfig(
            brokerAddr, 10000);

        SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(
            brokerAddr, 10000);

        BrokerStatsData transStatsData = null;

        // An exception indicates that it has not been used
        try {
            transStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                BrokerStatsManager.TOPIC_PUT_NUMS,
                TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        } catch (MQClientException e) {
            System.out.printf(e.getErrorMessage() + "\n");
        }

        BrokerStatsData scheduleStatsData = null;
        try {
            scheduleStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                BrokerStatsManager.TOPIC_PUT_NUMS, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
        } catch (MQClientException e) {
            System.out.printf(e.getErrorMessage() + "\n");
        }

        Map<String, Object> runtimeQuotaMap = new HashMap<>();
        //disk use ratio
        Map<String, Object> diskRatioMap = new HashMap<>();
        diskRatioMap.put("commitLogDiskRatio", kvTable.getTable().get("commitLogDiskRatio"));
        diskRatioMap.put("consumeQueueDiskRatio", kvTable.getTable().get("consumeQueueDiskRatio"));
        runtimeQuotaMap.put("diskRatio", diskRatioMap);

        //inTps and outTps
        Map<String, Object> tpsMap = new HashMap<>();
        double normalInTps = 0;
        double normalOutTps = 0;
        String putTps = kvTable.getTable().get("putTps");
        String getTransferedTps = kvTable.getTable().get("getTransferedTps");
        String[] inTpss = putTps.split(" ");
        if (inTpss.length > 0) {
            normalInTps = Double.parseDouble(inTpss[0]);
        }

        String[] outTpss = getTransferedTps.split(" ");
        if (outTpss.length > 0) {
            normalOutTps = Double.parseDouble(outTpss[0]);
        }

        double transInTps = null != transStatsData ? transStatsData.getStatsDay().getTps() : 0.0;
        double scheduleInTps = null != scheduleStatsData ? scheduleStatsData.getStatsDay().getTps()
            : 0.0;

        // order message
        double orderInTps = 0;
        for (Map.Entry<String, TopicConfig> entry : topicConfigSerializeWrapper.getTopicConfigTable()
            .entrySet()) {
            if (entry.getValue().isOrder()) {
                try {
                    BrokerStatsData brokerStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                        BrokerStatsManager.TOPIC_PUT_NUMS, entry.getValue().getTopicName());
                    orderInTps += brokerStatsData.getStatsDay().getTps();
                } catch (MQClientException e) {
                }

            }
        }

        tpsMap.put("normalInTps", normalInTps);
        tpsMap.put("normalOutTps", normalOutTps);
        tpsMap.put("transInTps", transInTps);
        tpsMap.put("transOutTps", 0.0);
        tpsMap.put("scheduleInTps", scheduleInTps);
        tpsMap.put("scheduleOutTps", 0.0);
        tpsMap.put("orderInTps", orderInTps);
        tpsMap.put("orderOutTps", 0.0);
        runtimeQuotaMap.put("tps", tpsMap);

        // putMessageAverageSize 平均
        runtimeQuotaMap.put("messageAverageSize", kvTable.getTable().get("putMessageAverageSize"));

        //topicSize
        runtimeQuotaMap.put("topicSize", topicConfigSerializeWrapper.getTopicConfigTable().size());
        runtimeQuotaMap.put("groupSize", subscriptionGroupWrapper.getSubscriptionGroupTable().size());
        return runtimeQuotaMap;
    }

    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "appstore-500.gz00b.dev.alipay.net:9876");
        MQAdminStartup.main(
            new String[] {new ExportBrokerRuntimeInfoCommand().commandName(), "-c", "DefaultCluster"});
    }
}
