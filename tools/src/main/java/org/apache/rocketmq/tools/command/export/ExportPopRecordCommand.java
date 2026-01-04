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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportPopRecordCommand implements SubCommand {

    @Override
    public String commandName() {
        return "exportPopRecord";
    }

    @Override
    public String commandDesc() {
        return "Export pop consumer record";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option(
            "c", "clusterName", true, "choose one cluster to export");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "choose one broker to export");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "dryRun", true, "no actual changes will be made");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            adminExt.start();
            boolean dryRun = commandLine.hasOption('d') &&
                Boolean.FALSE.toString().equalsIgnoreCase(commandLine.getOptionValue('d'));
            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                String brokerName = adminExt.getBrokerConfig(brokerAddr).getProperty("brokerName");
                export(adminExt, brokerAddr, brokerName, dryRun);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
                if (clusterInfo != null) {
                    Set<String> brokerNameSet = clusterInfo.getClusterAddrTable().get(clusterName);
                    if (brokerNameSet != null) {
                        brokerNameSet.forEach(brokerName -> {
                            BrokerData brokerData = clusterInfo.getBrokerAddrTable().get(brokerName);
                            if (brokerData != null) {
                                brokerData.getBrokerAddrs().forEach(
                                    (brokerId, brokerAddr) -> export(adminExt, brokerAddr, brokerName, dryRun));
                            }
                        });
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }

    private void export(DefaultMQAdminExt adminExt, String brokerAddr, String brokerName, boolean dryRun) {
        try {
            if (!dryRun) {
                adminExt.exportPopRecords(brokerAddr, TimeUnit.SECONDS.toMillis(30));
            }
            System.out.printf("Export broker records, " +
                "brokerName=%s, brokerAddr=%s, dryRun=%s%n", brokerName, brokerAddr, dryRun);
        } catch (Exception e) {
            System.out.printf("Export broker records error, " +
                "brokerName=%s, brokerAddr=%s, dryRun=%s%n%s", brokerName, brokerAddr, dryRun, e);
        }
    }
}

