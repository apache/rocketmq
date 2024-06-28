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
package org.apache.rocketmq.tools.command.cluster;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ClusterListSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "clusterList";
    }

    @Override
    public String commandDesc() {
        return "List cluster infos.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("m", "moreStats", false, "Print more stats");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "interval", true, "specify intervals numbers, it is in seconds");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        long printInterval = 1;
        boolean enableInterval = commandLine.hasOption('i');

        if (enableInterval) {
            printInterval = Long.parseLong(commandLine.getOptionValue('i')) * 1000;
        }

        String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : "";

        try {
            defaultMQAdminExt.start();
            long i = 0;

            do {
                if (i++ > 0) {
                    Thread.sleep(printInterval);
                }

                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();

                Set<String> clusterNames = getTargetClusterNames(clusterName, clusterInfo);

                if (commandLine.hasOption('m')) {
                    this.printClusterMoreStats(clusterNames, defaultMQAdminExt, clusterInfo);
                } else {
                    this.printClusterBaseInfo(clusterNames, defaultMQAdminExt, clusterInfo);
                }
            }
            while (enableInterval);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private Set<String> getTargetClusterNames(String clusterName, ClusterInfo clusterInfo) {
        if (StringUtils.isEmpty(clusterName)) {
            return clusterInfo.getClusterAddrTable().keySet();
        } else {
            Set<String> clusterNames = new TreeSet<>();
            clusterNames.add(clusterName);
            return clusterNames;
        }
    }

    private void printClusterMoreStats(final Set<String> clusterNames,
        final DefaultMQAdminExt defaultMQAdminExt,
        final ClusterInfo clusterInfo) {
        System.out.printf("%-16s  %-32s %14s %14s %14s %14s%n",
            "#Cluster Name",
            "#Broker Name",
            "#InTotalYest",
            "#OutTotalYest",
            "#InTotalToday",
            "#OutTotalToday"
        );

        for (String clusterName : clusterNames) {
            TreeSet<String> brokerNameTreeSet = new TreeSet<>();
            Set<String> brokerNameSet = clusterInfo.getClusterAddrTable().get(clusterName);
            if (brokerNameSet != null && !brokerNameSet.isEmpty()) {
                brokerNameTreeSet.addAll(brokerNameSet);
            }

            for (String brokerName : brokerNameTreeSet) {
                BrokerData brokerData = clusterInfo.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    Iterator<Map.Entry<Long, String>> itAddr = brokerData.getBrokerAddrs().entrySet().iterator();
                    while (itAddr.hasNext()) {
                        Map.Entry<Long, String> next1 = itAddr.next();
                        long inTotalYest = 0;
                        long outTotalYest = 0;
                        long inTotalToday = 0;
                        long outTotalToday = 0;

                        try {
                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            String msgPutTotalYesterdayMorning = kvTable.getTable().get("msgPutTotalYesterdayMorning");
                            String msgPutTotalTodayMorning = kvTable.getTable().get("msgPutTotalTodayMorning");
                            String msgPutTotalTodayNow = kvTable.getTable().get("msgPutTotalTodayNow");
                            String msgGetTotalYesterdayMorning = kvTable.getTable().get("msgGetTotalYesterdayMorning");
                            String msgGetTotalTodayMorning = kvTable.getTable().get("msgGetTotalTodayMorning");
                            String msgGetTotalTodayNow = kvTable.getTable().get("msgGetTotalTodayNow");

                            inTotalYest = Long.parseLong(msgPutTotalTodayMorning) - Long.parseLong(msgPutTotalYesterdayMorning);
                            outTotalYest = Long.parseLong(msgGetTotalTodayMorning) - Long.parseLong(msgGetTotalYesterdayMorning);

                            inTotalToday = Long.parseLong(msgPutTotalTodayNow) - Long.parseLong(msgPutTotalTodayMorning);
                            outTotalToday = Long.parseLong(msgGetTotalTodayNow) - Long.parseLong(msgGetTotalTodayMorning);

                        } catch (Exception ignored) {
                        }

                        System.out.printf("%-16s  %-32s %14d %14d %14d %14d%n",
                            clusterName,
                            brokerName,
                            inTotalYest,
                            outTotalYest,
                            inTotalToday,
                            outTotalToday
                        );
                    }
                }
            }
        }
    }

    private void printClusterBaseInfo(final Set<String> clusterNames,
                                      final DefaultMQAdminExt defaultMQAdminExt,
                                      final ClusterInfo clusterInfo) {
        System.out.printf("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %16s  %-22s  %-11s  %-12s  %-8s  %-10s%n",
            "#Cluster Name",
            "#Broker Name",
            "#BID",
            "#Addr",
            "#Version",
            "#InTPS(LOAD)",
            "#OutTPS(LOAD)",
            "#Timer(Progress)",
            "#PCWait(ms)",
            "#Hour",
            "#SPACE",
            "#ACTIVATED"
        );

        for (String clusterName : clusterNames) {
            TreeSet<String> brokerNameTreeSet = new TreeSet<>();
            Set<String> brokerNameSet = clusterInfo.getClusterAddrTable().get(clusterName);
            if (brokerNameSet != null && !brokerNameSet.isEmpty()) {
                brokerNameTreeSet.addAll(brokerNameSet);
            }

            for (String brokerName : brokerNameTreeSet) {
                BrokerData brokerData = clusterInfo.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {
                    Iterator<Map.Entry<Long, String>> itAddr = brokerData.getBrokerAddrs().entrySet().iterator();
                    while (itAddr.hasNext()) {
                        Map.Entry<Long, String> next1 = itAddr.next();
                        double in = 0;
                        double out = 0;
                        String version = "";
                        String sendThreadPoolQueueSize = "";
                        String pullThreadPoolQueueSize = "";
                        String ackThreadPoolQueueSize = "";
                        String sendThreadPoolQueueHeadWaitTimeMills = "";
                        String pullThreadPoolQueueHeadWaitTimeMills = "";
                        String ackThreadPoolQueueHeadWaitTimeMills = "";
                        String pageCacheLockTimeMills = "";
                        String earliestMessageTimeStamp = "";
                        String commitLogDiskRatio = "";
                        long timerReadBehind = 0;
                        long timerOffsetBehind = 0;
                        long timerCongestNum = 0;
                        float timerEnqueueTps = 0.0f;
                        float timerDequeueTps = 0.0f;
                        boolean isBrokerActive = false;
                        try {
                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            isBrokerActive = Boolean.parseBoolean(kvTable.getTable().get("brokerActive"));
                            String putTps = kvTable.getTable().get("putTps");
                            String getTransferredTps = kvTable.getTable().get("getTransferredTps");

                            sendThreadPoolQueueSize = kvTable.getTable().get("sendThreadPoolQueueSize");
                            pullThreadPoolQueueSize = kvTable.getTable().get("pullThreadPoolQueueSize");
                            ackThreadPoolQueueSize = kvTable.getTable().get("ackThreadPoolQueueSize");

                            sendThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("sendThreadPoolQueueHeadWaitTimeMills");
                            pullThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("pullThreadPoolQueueHeadWaitTimeMills");
                            ackThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("ackThreadPoolQueueHeadWaitTimeMills");
                            pageCacheLockTimeMills = kvTable.getTable().get("pageCacheLockTimeMills");
                            earliestMessageTimeStamp = kvTable.getTable().get("earliestMessageTimeStamp");
                            commitLogDiskRatio = kvTable.getTable().get("commitLogDiskRatio");

                            try {
                                timerReadBehind = Long.parseLong(kvTable.getTable().get("timerReadBehind"));
                                timerOffsetBehind = Long.parseLong(kvTable.getTable().get("timerOffsetBehind"));
                                timerCongestNum = Long.parseLong(kvTable.getTable().get("timerCongestNum"));
                                timerEnqueueTps = Float.parseFloat(kvTable.getTable().get("timerEnqueueTps"));
                                timerDequeueTps = Float.parseFloat(kvTable.getTable().get("timerDequeueTps"));
                            } catch (Throwable ignored) {
                            }

                            version = kvTable.getTable().get("brokerVersionDesc");

                            if (StringUtils.isNotBlank(putTps)) {
                                String[] tpss = putTps.split(" ");
                                if (tpss.length > 0) {
                                    in = Double.parseDouble(tpss[0]);
                                }
                            }

                            if (StringUtils.isNotBlank(getTransferredTps)) {
                                String[] tpss = getTransferredTps.split(" ");
                                if (tpss.length > 0) {
                                    out = Double.parseDouble(tpss[0]);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        double hour = 0.0;
                        double space = 0.0;

                        if (earliestMessageTimeStamp != null && earliestMessageTimeStamp.length() > 0) {
                            long mills = System.currentTimeMillis() - Long.parseLong(earliestMessageTimeStamp);
                            hour = mills / 1000.0 / 60.0 / 60.0;
                        }

                        if (commitLogDiskRatio != null && commitLogDiskRatio.length() > 0) {
                            space = Double.parseDouble(commitLogDiskRatio);
                        }

                        System.out.printf("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %24s  %-22s  %11s  %-12s  %-8s  %10s%n",
                            clusterName,
                            brokerName,
                            next1.getKey(),
                            next1.getValue(),
                            version,
                            String.format("%9.2f(%s,%sms)", in, sendThreadPoolQueueSize, sendThreadPoolQueueHeadWaitTimeMills),
                            String.format("%9.2f(%s,%sms | %s,%sms)", out, pullThreadPoolQueueSize, pullThreadPoolQueueHeadWaitTimeMills, ackThreadPoolQueueSize, ackThreadPoolQueueHeadWaitTimeMills),
                            String.format("%d-%d(%.1fw, %.1f, %.1f)", timerReadBehind, timerOffsetBehind, timerCongestNum / 10000.0f, timerEnqueueTps, timerDequeueTps),
                            pageCacheLockTimeMills,
                            String.format("%2.2f", hour),
                            String.format("%.4f", space),
                            isBrokerActive
                        );
                    }
                }
            }
        }
    }
}
