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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.stats.StatsAllSubCommand;

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

            Map<String, Map<String, Map<String, Map<String, Object>>>> evaluateReportMap = new HashMap<>();
            Map<String, Double> totalTpsMap = new HashMap<>();
            Map<String, Long> totalOneDayNumMap = new HashMap<>();
            initTotaMap(totalTpsMap, totalOneDayNumMap);

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

                        SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getUserSubscriptionGroup(
                            next1.getValue(), 10000);

                        Map<String, Map<String, Object>> brokerInfo = new HashMap<>();

                        //broker environment,machine configuration
                        brokerInfo.put("runtimeEnv", getRuntimeEnv(kvTable, properties));

                        //broker config
                        brokerInfo.put("brokerConfig", getConfig(properties, brokerData));

                        brokerInfo.put("runtimeQuota",
                            getRuntimeQuota(kvTable, defaultMQAdminExt, next1.getValue(), totalTpsMap,
                                totalOneDayNumMap, next1.getKey(), subscriptionGroupWrapper));

                        // runtime version
                        brokerInfo.put("runtimeVersion",
                            getRuntimeVersion(defaultMQAdminExt, subscriptionGroupWrapper));

                        brokerMap.put(next1.getValue(), brokerInfo);
                    }
                }
                evaluateReportMap.put(brokerName, brokerMap);
            }

            String path = filePath + "/brokerInfo.json";

            Map<String, Object> totalData = new HashMap<>();
            totalData.put("totalTps", totalTpsMap);
            totalData.put("totalOneDayNum", totalOneDayNumMap);

            Map<String, Object> result = new HashMap<>();
            result.put("evaluateReport", evaluateReportMap);
            result.put("totalData", totalData);

            MixAll.string2FileNotSafe(JSON.toJSONString(result, true), path);
            System.out.printf("export %s success", path);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }

    }

    private Map<String, Object> getRuntimeVersion(DefaultMQAdminExt defaultMQAdminExt,
        SubscriptionGroupWrapper subscriptionGroupWrapper) {
        Map<String, Object> runtimeVersionMap = new HashMap();

        Set<String> clientInfoSet = new HashSet<>();
        for (Map.Entry<String, SubscriptionGroupConfig> entry : subscriptionGroupWrapper
            .getSubscriptionGroupTable().entrySet()) {
            try {
                ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(
                    entry.getValue().getGroupName());
                for (Connection conn : cc.getConnectionSet()) {
                    String clientInfo = conn.getLanguage() + "%" + MQVersion.getVersionDesc(conn.getVersion());
                    clientInfoSet.add(clientInfo);
                }
            } catch (Exception e) {
                continue;
            }
        }
        runtimeVersionMap.put("rocketmqVersion", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        runtimeVersionMap.put("clientInfo", clientInfoSet);
        return runtimeVersionMap;
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

    private Map<String, Object> getRuntimeQuota(KVTable kvTable, DefaultMQAdminExt defaultMQAdminExt, String brokerAddr,
        Map<String, Double> totalTpsMap, Map<String, Long> totalOneDayNumMap, long brokerId,
        SubscriptionGroupWrapper subscriptionGroupWrapper)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getUserTopicConfig(
            brokerAddr, 10000);

        BrokerStatsData transStatsData = null;

        try {
            transStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                BrokerStatsManager.TOPIC_PUT_NUMS,
                TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        } catch (MQClientException e) {
        }

        BrokerStatsData scheduleStatsData = null;
        try {
            scheduleStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                BrokerStatsManager.TOPIC_PUT_NUMS, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
        } catch (MQClientException e) {
        }

        Map<String, Object> runtimeQuotaMap = new HashMap<>();
        //disk use ratio
        Map<String, Object> diskRatioMap = new HashMap<>();
        diskRatioMap.put("commitLogDiskRatio", kvTable.getTable().get("commitLogDiskRatio"));
        diskRatioMap.put("consumeQueueDiskRatio", kvTable.getTable().get("consumeQueueDiskRatio"));
        runtimeQuotaMap.put("diskRatio", diskRatioMap);

        //inTps and outTps
        Map<String, Double> tpsMap = new HashMap<>();
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

        double transInTps = null != transStatsData ? transStatsData.getStatsMinute().getTps() : 0.0;
        double scheduleInTps = null != scheduleStatsData ? scheduleStatsData.getStatsMinute().getTps() : 0.0;

        long transOneDayNum = null != transStatsData ? transStatsData.getStatsDay().getSum() : 0;
        long scheduleOneDayNum = null != scheduleStatsData ? scheduleStatsData.getStatsDay().getSum() : 0;

        // order message
        double orderInTps = 0;
        double orderOutTps = 0;
        long orderOneDayNum = 0;
        for (Map.Entry<String, TopicConfig> entry : topicConfigSerializeWrapper.getTopicConfigTable()
            .entrySet()) {
            if (entry.getValue().isOrder()) {
                try {
                    BrokerStatsData orderInBsd = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                        BrokerStatsManager.TOPIC_PUT_NUMS, entry.getValue().getTopicName());
                    orderInTps += orderInBsd.getStatsMinute().getTps();

                    orderOneDayNum += StatsAllSubCommand.compute24HourSum(orderInBsd);

                    GroupList groupList = defaultMQAdminExt.queryTopicConsumeByWho(entry.getValue().getTopicName());
                    for (String group : groupList.getGroupList()) {
                        String statsKey = String.format("%s@%s", entry.getValue().getTopicName(), group);
                        BrokerStatsData orderOutBsd = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                            BrokerStatsManager.GROUP_GET_NUMS, statsKey);
                        orderOutTps += orderOutBsd.getStatsMinute().getTps();
                    }
                } catch (Exception e) {
                }
            }
        }

        //current minute tps
        tpsMap.put("normalInTps", normalInTps);
        tpsMap.put("normalOutTps", normalOutTps);
        tpsMap.put("transInTps", transInTps);
        tpsMap.put("transOutTps", 0.0);
        tpsMap.put("scheduleInTps", scheduleInTps);
        tpsMap.put("scheduleOutTps", 0.0);
        tpsMap.put("orderInTps", orderInTps);
        tpsMap.put("orderOutTps", orderOutTps);
        runtimeQuotaMap.put("tps", tpsMap);

        //one day num
        Map<String, Long> oneDayNumMap = new HashMap<>();
        long normalOneDayNum = Long.parseLong(kvTable.getTable().get("msgPutTotalTodayNow")) -
            Long.parseLong(kvTable.getTable().get("msgPutTotalTodayMorning"));
        oneDayNumMap.put("normalOneDayNum", normalOneDayNum);
        oneDayNumMap.put("transOneDayNum", transOneDayNum);
        oneDayNumMap.put("scheduleOneDayNum", scheduleOneDayNum);
        oneDayNumMap.put("orderOneDayNum", orderOneDayNum);
        runtimeQuotaMap.put("oneDayNum", oneDayNumMap);

        if (brokerId == 0) {
            //all broker current minute tps
            totalTpsMap.put("totalNormalInTps", totalTpsMap.get("totalNormalInTps") + normalInTps);
            totalTpsMap.put("totalNormalOutTps", totalTpsMap.get("totalNormalOutTps") + normalOutTps);
            totalTpsMap.put("totalTransInTps", totalTpsMap.get("totalTransInTps") + transInTps);
            totalTpsMap.put("totalTransOutTps", totalTpsMap.get("totalTransOutTps"));
            totalTpsMap.put("totalScheduleInTps", totalTpsMap.get("totalScheduleInTps") + scheduleInTps);
            totalTpsMap.put("totalScheduleOutTps", totalTpsMap.get("totalScheduleOutTps"));
            totalTpsMap.put("totalOrderInTps", totalTpsMap.get("totalOrderInTps") + orderInTps);
            totalTpsMap.put("totalOrderOutTps", totalTpsMap.get("totalOrderOutTps") + orderOutTps);

            //all broker one day num
            totalOneDayNumMap.put("normalOneDayNum", totalOneDayNumMap.get("normalOneDayNum") + normalOneDayNum);
            totalOneDayNumMap.put("transOneDayNum", totalOneDayNumMap.get("transOneDayNum") + transOneDayNum);
            totalOneDayNumMap.put("scheduleOneDayNum", totalOneDayNumMap.get("scheduleOneDayNum") + scheduleOneDayNum);
            totalOneDayNumMap.put("orderOneDayNum", totalOneDayNumMap.get("orderOneDayNum") + orderOneDayNum);
        }

        // putMessageAverageSize 平均
        runtimeQuotaMap.put("messageAverageSize", kvTable.getTable().get("putMessageAverageSize"));

        //topicSize
        runtimeQuotaMap.put("topicSize", topicConfigSerializeWrapper.getTopicConfigTable().size());
        runtimeQuotaMap.put("groupSize", subscriptionGroupWrapper.getSubscriptionGroupTable().size());
        return runtimeQuotaMap;
    }

    private void initTotaMap(Map<String, Double> totalTpsMap, Map<String, Long> totalOneDayNumMap) {
        totalTpsMap.put("totalNormalInTps", 0.0);
        totalTpsMap.put("totalNormalOutTps", 0.0);
        totalTpsMap.put("totalTransInTps", 0.0);
        totalTpsMap.put("totalTransOutTps", 0.0);
        totalTpsMap.put("totalScheduleInTps", 0.0);
        totalTpsMap.put("totalScheduleOutTps", 0.0);
        totalTpsMap.put("totalOrderInTps", 0.0);
        totalTpsMap.put("totalOrderOutTps", 0.0);

        totalOneDayNumMap.put("normalOneDayNum", 0L);
        totalOneDayNumMap.put("transOneDayNum", 0L);
        totalOneDayNumMap.put("scheduleOneDayNum", 0L);
        totalOneDayNumMap.put("orderOneDayNum", 0L);
    }
}
