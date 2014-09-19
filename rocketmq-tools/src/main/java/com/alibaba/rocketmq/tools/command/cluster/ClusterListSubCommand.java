/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.cluster;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 查看集群信息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
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

        return options;
    }


    private void printClusterBaseInfo(final DefaultMQAdminExt defaultMQAdminExt)
            throws RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
            InterruptedException, MQBrokerException {

        ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();

        System.out.printf("%-16s  %-32s  %-4s  %-22s %-22s %11s %11s\n",//
            "#Cluster Name",//
            "#Broker Name",//
            "#BID",//
            "#Addr",//
            "#Version",//
            "#InTPS",//
            "#OutTPS"//
        );

        Iterator<Map.Entry<String, Set<String>>> itCluster =
                clusterInfoSerializeWrapper.getClusterAddrTable().entrySet().iterator();
        while (itCluster.hasNext()) {
            Map.Entry<String, Set<String>> next = itCluster.next();
            String clusterName = next.getKey();
            TreeSet<String> brokerNameSet = new TreeSet<String>();
            brokerNameSet.addAll(next.getValue());

            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {

                    Iterator<Map.Entry<Long, String>> itAddr =
                            brokerData.getBrokerAddrs().entrySet().iterator();
                    while (itAddr.hasNext()) {
                        Map.Entry<Long, String> next1 = itAddr.next();
                        double in = 0;
                        double out = 0;
                        String version = "";

                        try {
                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            String putTps = kvTable.getTable().get("putTps");
                            String getTransferedTps = kvTable.getTable().get("getTransferedTps");
                            version = kvTable.getTable().get("brokerVersionDesc");
                            {
                                String[] tpss = putTps.split(" ");
                                if (tpss != null && tpss.length > 0) {
                                    in = Double.parseDouble(tpss[0]);
                                }
                            }

                            {
                                String[] tpss = getTransferedTps.split(" ");
                                if (tpss != null && tpss.length > 0) {
                                    out = Double.parseDouble(tpss[0]);
                                }
                            }
                        }
                        catch (Exception e) {
                        }

                        System.out.printf("%-16s  %-32s  %-4s  %-22s %-22s %11.2f %11.2f\n",//
                            clusterName,//
                            brokerName,//
                            next1.getKey().longValue(),//
                            next1.getValue(),//
                            version,//
                            in,//
                            out//
                            );
                    }
                }
            }

            if (itCluster.hasNext()) {
                System.out.println("");
            }
        }
    }


    private void printClusterMoreStats(final DefaultMQAdminExt defaultMQAdminExt)
            throws RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
            InterruptedException, MQBrokerException {

        ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();

        System.out.printf("%-16s  %-32s %14s %14s %14s %14s\n",//
            "#Cluster Name",//
            "#Broker Name",//
            "#InTotalYest",//
            "#OutTotalYest",//
            "#InTotalToday",//
            "#OutTotalToday"//
        );

        Iterator<Map.Entry<String, Set<String>>> itCluster =
                clusterInfoSerializeWrapper.getClusterAddrTable().entrySet().iterator();
        while (itCluster.hasNext()) {
            Map.Entry<String, Set<String>> next = itCluster.next();
            String clusterName = next.getKey();
            TreeSet<String> brokerNameSet = new TreeSet<String>();
            brokerNameSet.addAll(next.getValue());

            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = clusterInfoSerializeWrapper.getBrokerAddrTable().get(brokerName);
                if (brokerData != null) {

                    Iterator<Map.Entry<Long, String>> itAddr =
                            brokerData.getBrokerAddrs().entrySet().iterator();
                    while (itAddr.hasNext()) {
                        Map.Entry<Long, String> next1 = itAddr.next();
                        long InTotalYest = 0;
                        long OutTotalYest = 0;
                        long InTotalToday = 0;
                        long OutTotalToday = 0;

                        try {
                            KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(next1.getValue());
                            String msgPutTotalYesterdayMorning =
                                    kvTable.getTable().get("msgPutTotalYesterdayMorning");
                            String msgPutTotalTodayMorning =
                                    kvTable.getTable().get("msgPutTotalTodayMorning");
                            String msgPutTotalTodayNow = kvTable.getTable().get("msgPutTotalTodayNow");
                            String msgGetTotalYesterdayMorning =
                                    kvTable.getTable().get("msgGetTotalYesterdayMorning");
                            String msgGetTotalTodayMorning =
                                    kvTable.getTable().get("msgGetTotalTodayMorning");
                            String msgGetTotalTodayNow = kvTable.getTable().get("msgGetTotalTodayNow");

                            InTotalYest =
                                    Long.parseLong(msgPutTotalTodayMorning)
                                            - Long.parseLong(msgPutTotalYesterdayMorning);
                            OutTotalYest =
                                    Long.parseLong(msgGetTotalTodayMorning)
                                            - Long.parseLong(msgGetTotalYesterdayMorning);

                            InTotalToday =
                                    Long.parseLong(msgPutTotalTodayNow)
                                            - Long.parseLong(msgPutTotalTodayMorning);
                            OutTotalToday =
                                    Long.parseLong(msgGetTotalTodayNow)
                                            - Long.parseLong(msgGetTotalTodayMorning);

                        }
                        catch (Exception e) {
                        }

                        System.out.printf("%-16s  %-32s %14d %14d %14d %14d\n",//
                            clusterName,//
                            brokerName,//
                            InTotalYest,//
                            OutTotalYest,//
                            InTotalToday,//
                            OutTotalToday//
                            );
                    }
                }
            }

            if (itCluster.hasNext()) {
                System.out.println("");
            }
        }
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            if (commandLine.hasOption('m')) {
                this.printClusterMoreStats(defaultMQAdminExt);
            }
            else {
                this.printClusterBaseInfo(defaultMQAdminExt);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
