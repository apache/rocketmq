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

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.stats.Stats;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.stats.StatsAllSubCommand;

public class ExportMetricsCommand implements SubCommand {

    @Override
    public String commandName() {
        return "exportMetrics";
    }

    @Override
    public String commandDesc() {
        return "Export metrics.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "choose a cluster to export");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true,
            "export metrics.json path | default /tmp/rocketmq/export");
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
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/export" : commandLine.getOptionValue('f')
                .trim();

            defaultMQAdminExt.start();

            Map<String, Map<String, Map<String, Object>>> evaluateReportMap = new HashMap<>();
            Map<String, Double> totalTpsMap = new HashMap<>();
            Map<String, Long> totalOneDayNumMap = new HashMap<>();
            initTotalMap(totalTpsMap, totalOneDayNumMap);

            ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
            Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    String addr = brokerData.getBrokerAddrs().get(0L);

                    KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(addr);

                    Properties properties = defaultMQAdminExt.getBrokerConfig(addr);

                    SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getUserSubscriptionGroup(addr,
                        10000);

                    Map<String, Map<String, Object>> brokerInfo = new HashMap<>();

                    //broker environment,machine configuration
                    brokerInfo.put("runtimeEnv", getRuntimeEnv(kvTable, properties));

                    brokerInfo.put("runtimeQuota",
                        getRuntimeQuota(kvTable, defaultMQAdminExt, addr, totalTpsMap,
                            totalOneDayNumMap, subscriptionGroupWrapper));

                    // runtime version
                    brokerInfo.put("runtimeVersion",
                        getRuntimeVersion(defaultMQAdminExt, subscriptionGroupWrapper));

                    evaluateReportMap.put(brokerName, brokerInfo);
                }

            }

            String path = filePath + "/metrics.json";

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

    private Map<String, Object> getRuntimeQuota(KVTable kvTable, DefaultMQAdminExt defaultMQAdminExt, String brokerAddr,
        Map<String, Double> totalTpsMap, Map<String, Long> totalOneDayNumMap,
        SubscriptionGroupWrapper subscriptionGroupWrapper)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getUserTopicConfig(
            brokerAddr, false, 10000);

        BrokerStatsData transStatsData = null;

        try {
            transStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                Stats.TOPIC_PUT_NUMS,
                TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC);
        } catch (MQClientException e) {
        }

        BrokerStatsData scheduleStatsData = null;
        try {
            scheduleStatsData = defaultMQAdminExt.viewBrokerStatsData(brokerAddr,
                Stats.TOPIC_PUT_NUMS, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
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
        String getTransferredTps = kvTable.getTable().get("getTransferredTps");
        String[] inTpss = putTps.split(" ");
        if (inTpss.length > 0) {
            normalInTps = Double.parseDouble(inTpss[0]);
        }

        String[] outTpss = getTransferredTps.split(" ");
        if (outTpss.length > 0) {
            normalOutTps = Double.parseDouble(outTpss[0]);
        }

        double transInTps = null != transStatsData ? transStatsData.getStatsMinute().getTps() : 0.0;
        double scheduleInTps = null != scheduleStatsData ? scheduleStatsData.getStatsMinute().getTps() : 0.0;

        long transOneDayInNum = null != transStatsData ? StatsAllSubCommand.compute24HourSum(transStatsData) : 0;
        long scheduleOneDayInNum = null != scheduleStatsData ? StatsAllSubCommand.compute24HourSum(scheduleStatsData) : 0;

        //current minute tps
        tpsMap.put("normalInTps", normalInTps);
        tpsMap.put("normalOutTps", normalOutTps);
        tpsMap.put("transInTps", transInTps);
        tpsMap.put("scheduleInTps", scheduleInTps);
        runtimeQuotaMap.put("tps", tpsMap);

        //one day num
        Map<String, Long> oneDayNumMap = new HashMap<>();
        long normalOneDayInNum = Long.parseLong(kvTable.getTable().get("msgPutTotalTodayMorning")) -
            Long.parseLong(kvTable.getTable().get("msgPutTotalYesterdayMorning"));
        long normalOneDayOutNum = Long.parseLong(kvTable.getTable().get("msgGetTotalTodayMorning")) -
            Long.parseLong(kvTable.getTable().get("msgGetTotalYesterdayMorning"));
        oneDayNumMap.put("normalOneDayInNum", normalOneDayInNum);
        oneDayNumMap.put("normalOneDayOutNum", normalOneDayOutNum);
        oneDayNumMap.put("transOneDayInNum", transOneDayInNum);
        oneDayNumMap.put("scheduleOneDayInNum", scheduleOneDayInNum);
        runtimeQuotaMap.put("oneDayNum", oneDayNumMap);

        //all broker current minute tps
        totalTpsMap.put("totalNormalInTps", totalTpsMap.get("totalNormalInTps") + normalInTps);
        totalTpsMap.put("totalNormalOutTps", totalTpsMap.get("totalNormalOutTps") + normalOutTps);
        totalTpsMap.put("totalTransInTps", totalTpsMap.get("totalTransInTps") + transInTps);
        totalTpsMap.put("totalScheduleInTps", totalTpsMap.get("totalScheduleInTps") + scheduleInTps);


        //all broker one day num
        totalOneDayNumMap.put("normalOneDayInNum", totalOneDayNumMap.get("normalOneDayInNum") + normalOneDayInNum);
        totalOneDayNumMap.put("normalOneDayOutNum", totalOneDayNumMap.get("normalOneDayOutNum") + normalOneDayOutNum);
        totalOneDayNumMap.put("transOneDayInNum", totalOneDayNumMap.get("transOneDayInNum") + transOneDayInNum);
        totalOneDayNumMap.put("scheduleOneDayInNum", totalOneDayNumMap.get("scheduleOneDayInNum") + scheduleOneDayInNum);

        // putMessageAverageSize
        runtimeQuotaMap.put("messageAverageSize", kvTable.getTable().get("putMessageAverageSize"));

        //topicSize
        runtimeQuotaMap.put("topicSize", topicConfigSerializeWrapper.getTopicConfigTable().size());
        runtimeQuotaMap.put("groupSize", subscriptionGroupWrapper.getSubscriptionGroupTable().size());
        return runtimeQuotaMap;
    }

    private void initTotalMap(Map<String, Double> totalTpsMap, Map<String, Long> totalOneDayNumMap) {
        totalTpsMap.put("totalNormalInTps", 0.0);
        totalTpsMap.put("totalNormalOutTps", 0.0);
        totalTpsMap.put("totalTransInTps", 0.0);
        totalTpsMap.put("totalScheduleInTps", 0.0);

        totalOneDayNumMap.put("normalOneDayInNum", 0L);
        totalOneDayNumMap.put("normalOneDayOutNum", 0L);
        totalOneDayNumMap.put("transOneDayInNum", 0L);
        totalOneDayNumMap.put("scheduleOneDayInNum", 0L);
    }
}
