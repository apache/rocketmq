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
package org.apache.rocketmq.tools.command.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetSyncStateSetSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getSyncStateSet";
    }

    @Override
    public String commandDesc() {
        return "Fetch syncStateSet for target brokers";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "controllerAddress", true, "the address of controller");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerName", true, "which broker to fetch");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "interval", true, "the interval(second) of get info");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            if (commandLine.hasOption('i')) {
                String interval = commandLine.getOptionValue('i');
                int flushSecond = 3;
                if (interval != null && !interval.trim().equals("")) {
                    flushSecond = Integer.parseInt(interval);
                }

                defaultMQAdminExt.start();

                while (true) {
                    this.innerExec(commandLine, options, defaultMQAdminExt);
                    Thread.sleep(flushSecond * 1000);
                }
            } else {
                defaultMQAdminExt.start();

                this.innerExec(commandLine, options, defaultMQAdminExt);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void innerExec(CommandLine commandLine, Options options,
        DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        String controllerAddress = commandLine.getOptionValue('a').trim().split(";")[0];
        if (commandLine.hasOption('b')) {
            String brokerName = commandLine.getOptionValue('b').trim();
            final ArrayList<String> brokers = new ArrayList<>();
            brokers.add(brokerName);
            printData(controllerAddress, brokers, defaultMQAdminExt);
        } else if (commandLine.hasOption('c')) {
            String clusterName = commandLine.getOptionValue('c').trim();
            Set<String> brokerNames = CommandUtil.fetchBrokerNameByClusterName(defaultMQAdminExt, clusterName);
            printData(controllerAddress, new ArrayList<>(brokerNames), defaultMQAdminExt);
        } else {
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        }
    }

    private void printData(String controllerAddress, List<String> brokerNames,
        DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        if (brokerNames.size() > 0) {
            final BrokerReplicasInfo brokerReplicasInfo = defaultMQAdminExt.getInSyncStateData(controllerAddress, brokerNames);
            final Map<String, BrokerReplicasInfo.ReplicasInfo> replicasInfoTable = brokerReplicasInfo.getReplicasInfoTable();
            for (Map.Entry<String, BrokerReplicasInfo.ReplicasInfo> next : replicasInfoTable.entrySet()) {
                final List<BrokerReplicasInfo.ReplicaIdentity> inSyncReplicas = next.getValue().getInSyncReplicas();
                final List<BrokerReplicasInfo.ReplicaIdentity> notInSyncReplicas = next.getValue().getNotInSyncReplicas();
                System.out.printf("\n#brokerName\t%s\n#MasterAddr\t%s\n#MasterEpoch\t%d\n#SyncStateSetEpoch\t%d\n#SyncStateSetNums\t%d\n",
                    next.getKey(), next.getValue().getMasterAddress(), next.getValue().getMasterEpoch(), next.getValue().getSyncStateSetEpoch(),
                    inSyncReplicas.size());
                for (BrokerReplicasInfo.ReplicaIdentity member : inSyncReplicas) {
                    System.out.printf("\n InSyncReplica:\t%s\n", member.toString());
                }

                for (BrokerReplicasInfo.ReplicaIdentity member : notInSyncReplicas) {
                    System.out.printf("\n NotInSyncReplica:\t%s\n", member.toString());
                }
            }
        }
    }
}
