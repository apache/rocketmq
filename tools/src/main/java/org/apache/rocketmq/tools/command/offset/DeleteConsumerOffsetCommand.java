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
package org.apache.rocketmq.tools.command.offset;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class DeleteConsumerOffsetCommand implements SubCommand {

    @Override
    public String commandName() {
        return "deleteConsumerOffset";
    }

    @Override
    public String commandDesc() {
        return "Delete consumer offset for a consumer group and topic.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        OptionGroup target = new OptionGroup();
        target.addOption(new Option("b", "brokerAddr", true, "delete consumer offset from which broker"));
        target.addOption(new Option("c", "clusterName", true, "delete consumer offset from which cluster"));
        target.setRequired(true);
        options.addOptionGroup(target);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String consumerGroup = commandLine.getOptionValue('g').trim();
            String topic = commandLine.getOptionValue('t').trim();
            adminExt.start();

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                adminExt.deleteConsumerOffset(brokerAddr, consumerGroup, topic);
                System.out.printf(
                    "delete consumer offset for group [%s] and topic [%s] from broker [%s] success.%n",
                    consumerGroup, topic, brokerAddr);
                return;
            }

            String clusterName = commandLine.getOptionValue('c').trim();
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
            for (String master : masterSet) {
                adminExt.deleteConsumerOffset(master, consumerGroup, topic);
                System.out.printf(
                    "delete consumer offset for group [%s] and topic [%s] from broker [%s] in cluster [%s] success.%n",
                    consumerGroup, topic, master, clusterName);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
