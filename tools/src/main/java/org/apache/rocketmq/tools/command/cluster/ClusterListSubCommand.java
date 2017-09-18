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
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
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
        return "List all of clusters";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("m", "moreStats", false, "Print more stats");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "interval", true, "specify intervals numbers, it is in seconds");
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

        try {
            defaultMQAdminExt.start();
            long i = 0;

            do {
                if (i++ > 0) {
                    Thread.sleep(printInterval);
                }
                if (commandLine.hasOption('m')) {
                    this.printClusterMoreStats(defaultMQAdminExt);
                } else {
                    this.printClusterBaseInfo(defaultMQAdminExt);
                }
            }
            while (enableInterval);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void printClusterMoreStats(final DefaultMQAdminExt defaultMQAdminExt) throws RemotingConnectException,
        RemotingTimeoutException, RemotingSendRequestException, InterruptedException, MQBrokerException {

        ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();

        System.out.printf("%-16s  %-32s %14s %14s %14s %14s%n",
            "#Cluster Name",
            "#Broker Name",
            "#InTotalYest",
            "#OutTotalYest",
            "#InTotalToday",
            "#OutTotalToday"
        );

        Iterator<Map.Entry<String, Set<String>>> itCluster = clusterInfoSerializeWrapper.getClusterAddrTable().entrySet().iterator();
        while (itCluster.hasNext()) {
            Map.Entry<String, Set<String>> next = itCluster.next();
            String clusterName = next.getKey();
            TreeSet<String> brokerNameSet = new TreeSet<String>();
            brokerNameSet.addAll(next.getValue());

            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
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

                        } catch (Exception e) {
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

            if (itCluster.hasNext()) {
                System.out.printf("");
            }
        }
    }

    private void printClusterBaseInfo(
        final DefaultMQAdminExt defaultMQAdminExt) throws RemotingConnectException, RemotingTimeoutException,
        RemotingSendRequestException, InterruptedException, MQBrokerException {

        ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();

        System.out.printf("%-16s  %-22s  %-4s  %-22s %-16s %19s %19s %10s %5s %6s%n",
            "#Cluster Name",
            "#Broker Name",
            "#BID",
            "#Addr",
            "#Version",
            "#InTPS(LOAD)",
            "#OutTPS(LOAD)",
            "#PCWait(ms)",
            "#Hour",
            "#SPACE"
        );

        Iterator<Map.Entry<String, Set<String>>> itCluster = clusterInfoSerializeWrapper.getClusterAddrTable().entrySet().iterator();
        while (itCluster.hasNext()) {
            Map.Entry<String, Set<String>> next = itCluster.next();
            String clusterName = next.getKey();
            TreeSet<String> brokerNameSet = new TreeSet<String>();
            brokerNameSet.addAll(next.getValue());

            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {

                    Iterator<Map.Entry<Long, String>> itAddr = brokerData.getBrokerAddrs().entrySet().iterator();
                    while (itAddr.hasNext()) {
                        Map.Entry<Long, String> next1 = itAddr.next();
                        double in = 0;
                        double out = 0;
                        String version = "";
                        String sendThreadPoolQueueSize = "";
                        String pullThreadPoolQueueSize = "";
                        String sendThreadPoolQueueHeadWaitTimeMills = "";
                        String pullThreadPoolQueueHeadWaitTimeMills = "";
                        String pageCacheLockTimeMills = "";
                        String earliestMessageTimeStamp = "";
                        String commitLogDiskRatio = "";
                        try {
                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            String putTps = kvTable.getTable().get("putTps");
                            String getTransferedTps = kvTable.getTable().get("getTransferedTps");
                            sendThreadPoolQueueSize = kvTable.getTable().get("sendThreadPoolQueueSize");
                            pullThreadPoolQueueSize = kvTable.getTable().get("pullThreadPoolQueueSize");

                            sendThreadPoolQueueSize = kvTable.getTable().get("sendThreadPoolQueueSize");
                            pullThreadPoolQueueSize = kvTable.getTable().get("pullThreadPoolQueueSize");

                            sendThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("sendThreadPoolQueueHeadWaitTimeMills");
                            pullThreadPoolQueueHeadWaitTimeMills = kvTable.getTable().get("pullThreadPoolQueueHeadWaitTimeMills");
                            pageCacheLockTimeMills = kvTable.getTable().get("pageCacheLockTimeMills");
                            earliestMessageTimeStamp = kvTable.getTable().get("earliestMessageTimeStamp");
                            commitLogDiskRatio = kvTable.getTable().get("commitLogDiskRatio");

                            version = kvTable.getTable().get("brokerVersionDesc");
                            {
                                String[] tpss = putTps.split(" ");
                                if (tpss.length > 0) {
                                    in = Double.parseDouble(tpss[0]);
                                }
                            }

                            {
                                String[] tpss = getTransferedTps.split(" ");
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
                            long mills = System.currentTimeMillis() - Long.valueOf(earliestMessageTimeStamp);
                            hour = mills / 1000.0 / 60.0 / 60.0;
                        }

                        if (commitLogDiskRatio != null && commitLogDiskRatio.length() > 0) {
                            space = Double.valueOf(commitLogDiskRatio);
                        }

                        System.out.printf("%-16s  %-22s  %-4s  %-22s %-16s %19s %19s %10s %5s %6s%n",
                            clusterName,
                            brokerName,
                            next1.getKey(),
                            next1.getValue(),
                            version,
                            String.format("%9.2f(%s,%sms)", in, sendThreadPoolQueueSize, sendThreadPoolQueueHeadWaitTimeMills),
                            String.format("%9.2f(%s,%sms)", out, pullThreadPoolQueueSize, pullThreadPoolQueueHeadWaitTimeMills),
                            pageCacheLockTimeMills,
                            String.format("%2.2f", hour),
                            String.format("%.4f", space)
                        );
                    }
                }
            }

            if (itCluster.hasNext()) {
                System.out.printf("");
            }
        }
    }
}
