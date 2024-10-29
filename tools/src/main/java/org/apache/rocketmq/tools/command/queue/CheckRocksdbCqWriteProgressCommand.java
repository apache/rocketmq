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

package org.apache.rocketmq.tools.command.queue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.CheckRocksdbCqWriteResult;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;

public class CheckRocksdbCqWriteProgressCommand implements SubCommand {

    @Override
    public String commandName() {
        return "checkRocksdbCqWriteProgress";
    }

    @Override
    public String commandDesc() {
        return "check if rocksdb cq is same as file cq";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "cluster", true, "cluster name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("n", "nameserverAddr", true, "nameserverAddr");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        options.addOption(opt);

        opt = new Option("cf", "checkFrom", true, "check from time");
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr(StringUtils.trim(commandLine.getOptionValue('n')));
        String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : "";
        String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "";
        // The default check is 30 days
        long checkStoreTime = commandLine.hasOption("cf")
            ? Long.parseLong(commandLine.getOptionValue("cf").trim())
            : System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30L);

        try {
            defaultMQAdminExt.start();
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Map<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
            if (clusterAddrTable.get(clusterName) == null) {
                System.out.print("clusterAddrTable is empty");
                return;
            }
            for (Map.Entry<String, BrokerData> entry : brokerAddrTable.entrySet()) {
                String brokerName = entry.getKey();
                BrokerData brokerData = entry.getValue();
                String brokerAddr = brokerData.getBrokerAddrs().get(0L);
                CheckRocksdbCqWriteResult result = defaultMQAdminExt.checkRocksdbCqWriteProgress(brokerAddr, topic, checkStoreTime);
                if (result.getCheckStatus() == CheckRocksdbCqWriteResult.CheckStatus.CHECK_ERROR.getValue()) {
                    System.out.print(brokerName + " check error, please check log... errInfo: " + result.getCheckResult());
                } else {
                    System.out.print(brokerName + " check doing, please wait and get the result from log... \n");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
