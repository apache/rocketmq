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

package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.HashMap;
import java.util.Set;

public class GetMasterSlaveDiffCommand implements SubCommand {

    private DefaultMQAdminExt defaultMQAdminExt;
    @Override
    public String commandName() {
        return "getMasterSlaveDiff";
    }

    @Override
    public String commandDesc() {
        return "Get number of bytes that slave falls behind master.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("b", "brokerName", true, "Broker Name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "Cluster Name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = createMQAdminExt(null);
        try {
            if (commandLine.hasOption('b')) {
                String brokerName = commandLine.getOptionValue('b').trim();
                printDiffForBroker(defaultMQAdminExt, brokerName);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                printDiffForCluster(defaultMQAdminExt, clusterName);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private DefaultMQAdminExt createMQAdminExt(RPCHook rpcHook) throws SubCommandException {
        if (this.defaultMQAdminExt != null) {
            return defaultMQAdminExt;
        } else {
            defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
            defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                defaultMQAdminExt.start();
            }
            catch (Exception e) {
                throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
            }
            return defaultMQAdminExt;
        }
    }

    private void printDiffForCluster(final DefaultMQAdminExt defaultMQAdminExt, String clusterName) throws SubCommandException {
        ClusterInfo clusterInfo = null;
        try {
            clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterMap = clusterInfo.getClusterAddrTable();
            Set<String> brokerNameSet = clusterMap.get(clusterName);
            if (brokerNameSet == null || brokerNameSet.isEmpty()) {
                System.out.printf("Can not find brokers for the cluster named %s!%n", clusterName);
                return;
            } else {
                System.out.printf("%-24s %-24s %-24s %-14s%n",
                        "#Cluster Name",
                        "#Broker Name",
                        "#Broker Address(Master)",
                        "#Diff(Bytes)"
                );
                for (String brokerName : brokerNameSet) {
                    HashMap<String/* brokerName */, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
                    BrokerData brokerData;
                    if ((brokerData = brokerAddrTable.get(brokerName)) == null) {
                        continue;
                    } else {
                        String masterBrokerAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                        if (StringUtils.isBlank(masterBrokerAddr))
                            continue;
                        try {
                            long diff = defaultMQAdminExt.getMasterSlaveDiff(masterBrokerAddr);
                            System.out.printf("%-24s %-24s %-24s %-14d%n", clusterName, brokerName, masterBrokerAddr, diff);
                        } catch (Exception e) {
                            continue;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }

    private void printDiffForBroker(final DefaultMQAdminExt defaultMQAdminExt, String brokerName) throws SubCommandException {
        ClusterInfo clusterInfoSerializeWrapper = null;
        try {
            clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
        HashMap<String/* brokerName */, BrokerData> brokerAddrTable = clusterInfoSerializeWrapper.getBrokerAddrTable();
        BrokerData brokerData;
        if ((brokerData = brokerAddrTable.get(brokerName)) == null) {
            System.out.printf("Can not find broker named %s!%n", brokerName);
        } else {
            String masterBrokerAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
            if (!StringUtils.isBlank(masterBrokerAddr)) {
                try {
                    System.out.printf("%-24s %-24s %-14s%n",
                            "#Broker Name",
                            "#Broker Address(Master)",
                            "#Diff(Bytes)"
                    );
                    long diff = defaultMQAdminExt.getMasterSlaveDiff(masterBrokerAddr);
                    System.out.printf("%-24s %-24s %-14d", brokerName, masterBrokerAddr, diff);
                } catch (Exception e) {
                    throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
                }
            } else {
                System.out.printf("The master broker named %s maybe down!%n", brokerName);
            }
        }
    }
}
