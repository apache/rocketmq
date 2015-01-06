package com.alibaba.rocketmq.tools.command.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.protocol.body.BrokerStatsData;
import com.alibaba.rocketmq.common.protocol.body.GroupList;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.MQAdminStartup;
import com.alibaba.rocketmq.tools.command.SubCommand;


public class StatsAllSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "statsAll";
    }


    @Override
    public String commandDesc() {
        return "Topic and Consumer tps stats";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "activeTopic", false, "print active topic only");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static long compute24HourSum(BrokerStatsData bsd) {
        if (bsd.getStatsDay().getSum() != 0) {
            return bsd.getStatsDay().getSum();
        }

        if (bsd.getStatsHour().getSum() != 0) {
            return bsd.getStatsHour().getSum();
        }

        if (bsd.getStatsMinute().getSum() != 0) {
            return bsd.getStatsMinute().getSum();
        }

        return 0;
    }


    public static void printTopicDetail(final DefaultMQAdminExt admin, final String topic,
            final boolean activeTopic) throws RemotingException, MQClientException, InterruptedException,
            MQBrokerException {
        TopicRouteData topicRouteData = admin.examineTopicRouteInfo(topic);

        GroupList groupList = admin.queryTopicConsumeByWho(topic);

        double inTPS = 0;

        long inMsgCntToday = 0;

        // 统计Topic写入
        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String masterAddr = bd.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (masterAddr != null) {
                try {
                    BrokerStatsData bsd =
                            admin.ViewBrokerStatsData(masterAddr, BrokerStatsManager.TOPIC_PUT_NUMS, topic);
                    inTPS += bsd.getStatsMinute().getTps();
                    inMsgCntToday += compute24HourSum(bsd);
                }
                catch (Exception e) {
                }
            }
        }

        if (groupList != null && !groupList.getGroupList().isEmpty()) {
            // 统计订阅
            for (String group : groupList.getGroupList()) {
                double outTPS = 0;
                long outMsgCntToday = 0;

                for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                    String masterAddr = bd.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        try {
                            String statsKey = String.format("%s@%s", topic, group);
                            BrokerStatsData bsd =
                                    admin.ViewBrokerStatsData(masterAddr, BrokerStatsManager.GROUP_GET_NUMS,
                                        statsKey);
                            outTPS += bsd.getStatsMinute().getTps();
                            outMsgCntToday += compute24HourSum(bsd);
                        }
                        catch (Exception e) {
                        }
                    }
                }

                if (!activeTopic || (inMsgCntToday > 0) || //
                        (outMsgCntToday > 0)) {
                    // 打印
                    System.out.printf("%-32s  %-32s %11.2f %11.2f %14d %14d\n",//
                        UtilAll.frontStringAtLeast(topic, 32),//
                        UtilAll.frontStringAtLeast(group, 32),//
                        inTPS,//
                        outTPS,//
                        inMsgCntToday,//
                        outMsgCntToday//
                        );
                }
            }
        }
        // 没有订阅者
        else {
            if (!activeTopic || (inMsgCntToday > 0)) {
                // 打印
                System.out.printf("%-32s  %-32s %11.2f %11s %14d %14s\n",//
                    UtilAll.frontStringAtLeast(topic, 32),//
                    "",//
                    inTPS,//
                    "",//
                    inMsgCntToday,//
                    "NO_CONSUMER"//
                );
            }
        }
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            TopicList topicList = defaultMQAdminExt.fetchAllTopicList();

            System.out.printf("%-32s  %-32s %11s %11s %14s %14s\n",//
                "#Topic",//
                "#Consumer Group",//
                "#InTPS",//
                "#OutTPS",//
                "#InMsg24Hour",//
                "#OutMsg24Hour"//
            );

            boolean activeTopic = commandLine.hasOption('a');

            for (String topic : topicList.getTopicList()) {
                if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                        || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                    continue;
                }

                try {
                    printTopicDetail(defaultMQAdminExt, topic, activeTopic);
                }
                catch (Exception e) {
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }


    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "10.101.87.102:9876");
        MQAdminStartup.main(new String[] { new StatsAllSubCommand().commandName() });
    }
}
