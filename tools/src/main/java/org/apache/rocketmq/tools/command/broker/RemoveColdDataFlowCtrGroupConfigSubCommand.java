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

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class RemoveColdDataFlowCtrGroupConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "removeColdDataFlowCtrGroupConfig";
    }

    @Override
    public String commandDesc() {
        return "remove consumer from cold ctr config";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "update which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "update which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "the consumer group will remove from the config");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String consumerGroup = commandLine.getOptionValue('g').trim();
            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                defaultMQAdminExt.removeColdDataFlowCtrGroupConfig(brokerAddr, consumerGroup);
                System.out.printf("remove broker cold read threshold success, %s\n", brokerAddr);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddr : masterSet) {
                    try {
                        defaultMQAdminExt.removeColdDataFlowCtrGroupConfig(brokerAddr, consumerGroup);
                        System.out.printf("remove broker cold read threshold success, %s\n", brokerAddr);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return;
            }
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
