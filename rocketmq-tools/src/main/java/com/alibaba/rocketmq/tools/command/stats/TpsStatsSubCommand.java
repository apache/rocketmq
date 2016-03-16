package com.alibaba.rocketmq.tools.command.stats;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.GroupList;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import com.taobao.tlog.client.JSONResult;
import com.taobao.tlog.client.KeyValueParam;
import com.taobao.tlog.client.KeyValueQuery;
import com.taobao.tlog.client.TLogQueryClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.*;


public class TpsStatsSubCommand implements SubCommand {

    private static final String PUT_TPS = "TOPIC_PUT_NUMS";
    private static final String GET_TPS = "GROUP_GET_NUMS";
    private static final String TLOG_DOMAIN = "http://110.75.84.129:9999";

    class TpsDataInfo implements Comparable<TpsDataInfo> {
        private String topicName;
        private String groupName;
        private int tps = 0;
        private String clusterName;


        @Override
        public int compareTo(TpsDataInfo o) {
            return o.tps - tps;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public String getGroupName() {
            return groupName;
        }

        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        public int getTps() {
            return tps;
        }

        public void setTps(int tps) {
            this.tps = tps;
        }

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }
    }

    @Override
    public String commandName() {
        return "tpsStats";
    }


    @Override
    public String commandDesc() {
        return "produce and consume tps stats from tlog";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "cluster name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "put tps", false, "show put tps");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "subscribe tps", false, "show consume tps");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "amount of list", true, "amount of list, default is all");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            int amount = -1;
            String clusterName = "";
            if (commandLine.hasOption('a')) {
                amount = Integer.parseInt(commandLine.getOptionValue('a').trim());
            }
            if (commandLine.hasOption('c')) {
                clusterName = commandLine.getOptionValue('c').trim();
            }
            //查询写入tps
            if (commandLine.hasOption('p')) {
                List<TpsDataInfo> tpsDataInfoList = new ArrayList<TpsDataInfo>();
                if (clusterName.length() > 0) {
                    getClusterPutTps(defaultMQAdminExt, clusterName, tpsDataInfoList);
                    printClusterTps(tpsDataInfoList, amount, PUT_TPS);
                } else {
                    ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
                    if (null == clusterInfo)
                        return;
                    for (String cluster : clusterInfo.getClusterAddrTable().keySet()) {
                        tpsDataInfoList.clear();
                        System.out.println("put tps for cluster [" + cluster + "]");
                        getClusterPutTps(defaultMQAdminExt, cluster, tpsDataInfoList);
                        printClusterTps(tpsDataInfoList, amount, PUT_TPS);
                        System.out.println();
                    }
                }
                return;
            }
            //查询消费tps
            else if (commandLine.hasOption('s')) {
                List<TpsDataInfo> tpsDataInfoList = new ArrayList<TpsDataInfo>();
                if (clusterName.length() > 0) {
                    getClusterGetTps(defaultMQAdminExt, clusterName, tpsDataInfoList);
                    printClusterTps(tpsDataInfoList, amount, GET_TPS);
                } else {
                    ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
                    if (null == clusterInfo)
                        return;
                    for (String cluster : clusterInfo.getClusterAddrTable().keySet()) {
                        tpsDataInfoList.clear();
                        System.out.println("consume tps for cluster [" + cluster + "]");
                        getClusterGetTps(defaultMQAdminExt, cluster, tpsDataInfoList);
                        printClusterTps(tpsDataInfoList, amount, PUT_TPS);
                        System.out.println();
                    }
                }
                return;
            }
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }


    private Set<String> getClusterTopics(DefaultMQAdminExt defaultMQAdminExt, String clusterName) throws Exception {
        Set<String> topics = new HashSet<String>();
        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        for (String topic : topicList.getTopicList()) {
            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                    || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                continue;
            }
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            BrokerData brokerData = topicRouteData.getBrokerDatas().get(0);
            String brokerName = brokerData.getBrokerName();
            Iterator<Map.Entry<String, Set<String>>> it = clusterInfo.getClusterAddrTable().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Set<String>> next = it.next();
                if (next.getValue().contains(brokerName)) {
                    if (next.getKey().equals(clusterName)) {
                        topics.add(topic);
                    }
                    break;
                }
            }
        }
        return topics;
    }


    private void getClusterPutTps(DefaultMQAdminExt defaultMQAdminExt, String clusterName, List<TpsDataInfo> tpsDataInfoList) throws Exception {
        Set<String> topics = getClusterTopics(defaultMQAdminExt, clusterName);
        for (String topic : topics) {
            TpsDataInfo tpsDataInfo = getTpsData(PUT_TPS, topic, clusterName, topic, "");
            if (null != tpsDataInfo)
                tpsDataInfoList.add(tpsDataInfo);
        }
    }

    private void getClusterGetTps(DefaultMQAdminExt defaultMQAdminExt, String clusterName, List<TpsDataInfo> tpsDataInfoList) throws Exception {
        ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
        Set<String> brokerNameSet = clusterInfoSerializeWrapper.getClusterAddrTable().get(clusterName);
        String brokerAddr = "";
        //找个broker取数据
        for (String brokerName : brokerNameSet) {
            BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
            if (null != brokerData)
                brokerAddr = brokerData.getBrokerAddrs().get(0L);
            break;
        }
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getAllTopicGroup(brokerAddr, 5000);
        for (String topic : topicConfigSerializeWrapper.getTopicConfigTable().keySet()) {
            GroupList groupList = defaultMQAdminExt.queryTopicConsumeByWho(topic);
            for (String group : groupList.getGroupList()) {
                TpsDataInfo tpsDataInfo = getTpsData(GET_TPS, topic + "@" + group, clusterName, topic, group);
                if (null != tpsDataInfo)
                    tpsDataInfoList.add(tpsDataInfo);
            }
        }
    }


    private TpsDataInfo getTpsData(String type, String key, String clusterName, String topic, String group) {
        long endTime = System.currentTimeMillis() - 60000;
        long startTime = endTime - 60000;
        KeyValueQuery kvq = new KeyValueQuery("metaq_metaqstats", // StageId
                "meta_stats_1min",// BizId
                new KeyValueParam("type", type), //type-发送tps
                new KeyValueParam("key", key), //topic or topic@group
                new KeyValueParam("date", startTime + "", endTime + ""));//kv3-范围值，格式为ms

        JSONResult queryData = TLogQueryClient.queryData(TLOG_DOMAIN, kvq); //查询url,query keys
        if (queryData != null) {
            JSONResult.JSONRecord[] records = queryData.getRecords();
            int tps = 0;
            TpsDataInfo tpsDataInfo = new TpsDataInfo();
            tpsDataInfo.setClusterName(clusterName);
            tpsDataInfo.setTopicName(topic);
            tpsDataInfo.setGroupName(group);
            if (records.length > 0)
                tps = (Integer) records[records.length - 1].getValueByKeyName("sum");
            tpsDataInfo.setTps(tps);
            return tpsDataInfo;
        }
        return null;
    }

    private void printClusterTps(List<TpsDataInfo> tpsDataInfoList, int amount, String type) throws Exception {
        if (tpsDataInfoList.size() > 0) {
            Collections.sort(tpsDataInfoList);
            if (type.equals(PUT_TPS)) {
                System.out.printf("%-64s  %11s\n",//
                        "#Topic Name",//
                        "#InTPS"
                );
            } else if (type.equals(GET_TPS)) {
                System.out.printf("%-64s  %64s  %11s\n",//
                        "#Group Name",//
                        "#Topic Name",//
                        "#InTPS"
                );
            }
            for (int i = 0; i < tpsDataInfoList.size(); i++) {
                TpsDataInfo tpsDataInfo = tpsDataInfoList.get(i);
                if (amount > 0 && i >= amount)
                    break;
                if (type.equals(PUT_TPS)) {
                    System.out.printf("%-64s  %11s\n",//
                            tpsDataInfo.getTopicName(),
                            tpsDataInfo.getTps()
                    );
                } else if (type.equals(GET_TPS)) {
                    System.out.printf("%-64s  %64s  %11s\n",//
                            tpsDataInfo.getGroupName(),
                            tpsDataInfo.getTopicName(),
                            tpsDataInfo.getTps()
                    );
                }
            }
        }
    }


}
