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

import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.remoting.protocol.body.EpochEntryCache;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetBrokerEpochSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getBrokerEpoch";
    }

    @Override
    public String commandDesc() {
        return "Fetch broker epoch entries";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterName", true, "which cluster");
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
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
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
        if (commandLine.hasOption('b')) {
            String brokerName = commandLine.getOptionValue('b').trim();
            final Set<String> brokers = CommandUtil.fetchMasterAndSlaveAddrByBrokerName(defaultMQAdminExt, brokerName);
            printData(brokers, defaultMQAdminExt);
        } else if (commandLine.hasOption('c')) {
            String clusterName = commandLine.getOptionValue('c').trim();
            Set<String> brokers = CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
            printData(brokers, defaultMQAdminExt);
        } else {
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        }
    }

    private void printData(Set<String> brokers, DefaultMQAdminExt defaultMQAdminExt) throws Exception {
        for (String brokerAddr : brokers) {
            final EpochEntryCache epochCache = defaultMQAdminExt.getBrokerEpochCache(brokerAddr);
            System.out.printf("\n#clusterName\t%s\n#brokerName\t%s\n#brokerAddr\t%s\n#brokerId\t%d",
                epochCache.getClusterName(), epochCache.getBrokerName(), brokerAddr, epochCache.getBrokerId());
            final List<EpochEntry> epochList = epochCache.getEpochList();
            for (int i = 0; i < epochList.size(); i++) {
                final EpochEntry epochEntry = epochList.get(i);
                if (i == epochList.size() - 1) {
                    epochEntry.setEndOffset(epochCache.getMaxOffset());
                }
                System.out.printf("\n#Epoch: %s", epochEntry.toString());
            }
            System.out.print("\n");
        }
    }
}
