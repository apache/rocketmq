/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.consumer;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.ConsumeStatsList;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import com.taobao.tlog.client.JSONResult;
import com.taobao.tlog.client.KeyValueParam;
import com.taobao.tlog.client.KeyValueQuery;
import com.taobao.tlog.client.TLogQueryClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

import java.util.*;


/**
 * Consume消息福布斯排行...
 *
 * @author zhouli
 * @since 2017-10-10
 */
public class ConsumeOffsetRankSubCommand implements SubCommand {
    private final Logger log = ClientLogger.getLog();

    public static void main(String[] args) {
        for (int i = 0; i < 1; ++i) {
            long endTime = System.currentTimeMillis() - 60000;
            long startTime = endTime - 60000;

            KeyValueQuery kvq = new KeyValueQuery("metaq_metaqstats", // StageId
                    "meta_stats_1min",// BizId
                    new KeyValueParam("type", "TOPIC_PUT_NUMS"), //kv1-定值
                    new KeyValueParam("key", "TRADE"), //kv2-定值
                    new KeyValueParam("date", startTime + "", endTime + ""));//kv3-范围值，格式为ms

            JSONResult queryData = TLogQueryClient.queryData("http://110.75.84.129:9999", kvq); //查询url,query keys
            long tps = 0;
            if (queryData != null) {
                JSONResult.JSONRecord[] records = queryData.getRecords();
                for (JSONResult.JSONRecord r : records) {
                    tps += (Long) r.getValueByKeyName("sum"); //从结果中获取sum这个key的value
                }
                System.out.println(tps);
            }
        }


    }

    @Override
    public String commandName() {
        return "consumeOffsetRank";
    }

    @Override
    public String commandDesc() {
        return "Query rank list of certain consume data";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "Broker address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "cluster name for showing consume offset");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "timeoutMillis", true, "request timeout Millis, default is 50000");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "amount of rank list", true, "amount of rank list, default is 10");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        Map<String/*group*/, ConsumeDataInfo> consumeDataInfoMap = new HashMap<String, ConsumeDataInfo>();
        long timeoutMillis = 50000;
        if (commandLine.hasOption('t')) {
            timeoutMillis = Long.parseLong(commandLine.getOptionValue('t').trim());
        }
        int amount = 10;
        if (commandLine.hasOption('a')) {
            amount = Integer.parseInt(commandLine.getOptionValue('a').trim());
        }
        try {
            defaultMQAdminExt.start();
            //查询单个节点
            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                List<ConsumeDataInfo> consumeDataInfoList = consumeRankInBroker(defaultMQAdminExt, brokerAddr, timeoutMillis);
                if (null != consumeDataInfoList)
                    mergeConsumeDataInfoList(consumeDataInfoMap, consumeDataInfoList);
                //打印结果
                printResult(defaultMQAdminExt, consumeDataInfoMap, amount);
            }
            //全部查询，按照集群展示
            else {
                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
                if (null == clusterInfo)
                    return;
                //查询一个集群
                if (commandLine.hasOption('c')) {
                    String clusterName = commandLine.getOptionValue('c').trim();
                    printClusterConsumeDataInfo(clusterInfo, clusterName, defaultMQAdminExt, timeoutMillis, amount, consumeDataInfoMap);
                }
                //查询全部集群
                else {
                    for (String clusterName : clusterInfo.getClusterAddrTable().keySet()) {
                        consumeDataInfoMap.clear();
                        try {
                            System.out.println("consume offset rank list for cluster [" + clusterName + "]");
                            printClusterConsumeDataInfo(clusterInfo, clusterName, defaultMQAdminExt, timeoutMillis, amount, consumeDataInfoMap);
                            System.out.println();
                        } catch (Exception e) {
                            //异常的cluster直接跳过，继续获取下一个cluster信息
                            System.out.println("get cluster [" + clusterName + "]" + "consume offset data error");
                            System.out.println();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private List<ConsumeDataInfo> consumeRankInBroker(DefaultMQAdminExt defaultMQAdminExt, String brokerAddr,
                                                      long timeoutMillis) throws Exception {
        ConsumeStatsList consumeStatsList = defaultMQAdminExt.fetchConsumeStatsInBroker(brokerAddr, false, timeoutMillis);
        List<ConsumeDataInfo> consumeDataInfoList = new ArrayList<ConsumeDataInfo>();
        for (Map<String, List<ConsumeStats>> map : consumeStatsList.getConsumeStatsList()) {
            for (String group : map.keySet()) {
                List<ConsumeStats> consumeStatsArray = map.get(group);
                for (ConsumeStats consumeStats : consumeStatsArray) {
                    ConsumeDataInfo consumeDataInfo = new ConsumeDataInfo();
                    consumeDataInfo.setGroup(group);
                    if (consumeStats != null) {
                        consumeDataInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                        consumeDataInfo.setDiffTotal(consumeStats.computeTotalDiff());
                    }
                    consumeDataInfoList.add(consumeDataInfo);
                }
            }
        }
        return consumeDataInfoList;
    }

    private void mergeConsumeDataInfoList(Map<String/*group*/, ConsumeDataInfo> consumeDataInfoMap, List<ConsumeDataInfo> consumeDataInfoList) {
        if (null == consumeDataInfoMap || null == consumeDataInfoList)
            return;
        for (ConsumeDataInfo consumeDataInfo : consumeDataInfoList) {
            ConsumeDataInfo mergeConsumeDataInfo = consumeDataInfoMap.get(consumeDataInfo.getGroup());
            if (null == mergeConsumeDataInfo) {
                mergeConsumeDataInfo = consumeDataInfo;
                consumeDataInfoMap.put(mergeConsumeDataInfo.getGroup(), mergeConsumeDataInfo);
            } else {
                mergeConsumeDataInfo.setDiffTotal(mergeConsumeDataInfo.getDiffTotal() + consumeDataInfo.getDiffTotal());
            }
        }
    }

    private void printResult(DefaultMQAdminExt defaultMQAdminExt, Map<String/*group*/, ConsumeDataInfo> consumeDataInfoMap, int amount) {
        List<ConsumeDataInfo> consumeDataInfoRet = new ArrayList<ConsumeDataInfo>();
        for (String group : consumeDataInfoMap.keySet()) {
            consumeDataInfoRet.add(consumeDataInfoMap.get(group));
        }
        Collections.sort(consumeDataInfoRet);
        System.out.printf("%-48s  %-6s  %-24s %-5s  %-14s  %-7s  %s\n",//
                "#Group",//
                "#Count",//
                "#Version",//
                "#Type",//
                "#Model",//
                "#TPS",//
                "#Diff Total"//
        );
        int onlineCount = 0;
        for (int i = 0; i < consumeDataInfoRet.size(); i++) {
            if (onlineCount == amount) {
                break;
            }
            ConsumeDataInfo consumeDataInfo = consumeDataInfoRet.get(i);
            ConsumerConnection cc = null;
            try {
                cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumeDataInfo.getGroup());
            } catch (Exception e) {
                log.error("examineConsumerConnectionInfo exception, " + consumeDataInfo.getGroup());
            }
            if (cc != null) {
                onlineCount++;
                consumeDataInfo.setCount(cc.getConnectionSet().size());
                consumeDataInfo.setMessageModel(cc.getMessageModel());
                consumeDataInfo.setConsumeType(cc.getConsumeType());
                consumeDataInfo.setVersion(cc.computeMinVersion());
                System.out.printf("%-48s  %-6d  %-24s %-5s  %-14s  %-7d  %d\n",//
                        UtilAll.frontStringAtLeast(consumeDataInfo.getGroup(), 32),//
                        consumeDataInfo.getCount(),//
                        consumeDataInfo.getCount() > 0 ? consumeDataInfo.versionDesc() : "OFFLINE",//
                        consumeDataInfo.consumeTypeDesc(),//
                        consumeDataInfo.messageModelDesc(),//
                        consumeDataInfo.getConsumeTps(),//
                        consumeDataInfo.getDiffTotal()//
                );
            }
        }
    }

    private void printClusterConsumeDataInfo(ClusterInfo clusterInfo, String clusterName, DefaultMQAdminExt defaultMQAdminExt,
                                             long timeoutMillis, int amount, Map<String/*group*/, ConsumeDataInfo> consumeDataInfoMap) throws Exception {
        Set<String> brokerNameSet = clusterInfo.getClusterAddrTable().get(clusterName);
        if (null == brokerNameSet)
            return;
        for (String brokerName : brokerNameSet) {
            String brokerAddr = clusterInfo.getBrokerAddrTable().get(brokerName).getBrokerAddrs().get(MixAll.MASTER_ID);
            if (null != brokerAddr && brokerAddr.length() > 0) {
                List<ConsumeDataInfo> consumeDataInfoList = consumeRankInBroker(defaultMQAdminExt, brokerAddr, timeoutMillis);
                mergeConsumeDataInfoList(consumeDataInfoMap, consumeDataInfoList);
            }
        }
        //打印结果
        printResult(defaultMQAdminExt, consumeDataInfoMap, amount);
    }

    class ConsumeDataInfo implements Comparable<ConsumeDataInfo> {
        private String group;
        private int version;
        private int count;
        private ConsumeType consumeType;
        private MessageModel messageModel;
        private int consumeTps;
        private long diffTotal;
        private String clusterName;

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String consumeTypeDesc() {
            if (this.count != 0) {
                return this.getConsumeType() == ConsumeType.CONSUME_ACTIVELY ? "PULL" : "PUSH";
            }
            return "";
        }

        public ConsumeType getConsumeType() {
            return consumeType;
        }

        public void setConsumeType(ConsumeType consumeType) {
            this.consumeType = consumeType;
        }

        public String messageModelDesc() {
            if (this.count != 0) {
                return this.getMessageModel().toString();
            }
            return "";
        }

        public MessageModel getMessageModel() {
            return messageModel;
        }

        public void setMessageModel(MessageModel messageModel) {
            this.messageModel = messageModel;
        }

        public String versionDesc() {
            if (this.count != 0) {
                return MQVersion.getVersionDesc(this.version);
            }
            return "";
        }

        @Override
        public int compareTo(ConsumeDataInfo o) {
            if (o.diffTotal == diffTotal)
                return 0;
            else if (o.diffTotal < diffTotal)
                return -1;
            else if (o.diffTotal > diffTotal)
                return 1;
            return 0;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public long getDiffTotal() {
            return diffTotal;
        }


        public void setDiffTotal(long diffTotal) {
            this.diffTotal = diffTotal;
        }

        public int getConsumeTps() {
            return consumeTps;
        }


        public void setConsumeTps(int consumeTps) {
            this.consumeTps = consumeTps;
        }


        public int getVersion() {
            return version;
        }


        public void setVersion(int version) {
            this.version = version;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }
    }

}
