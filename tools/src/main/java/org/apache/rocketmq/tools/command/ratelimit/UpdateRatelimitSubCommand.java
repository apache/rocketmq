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
package org.apache.rocketmq.tools.command.ratelimit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.RatelimitInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Set;

public class UpdateRatelimitSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateRatelimit";
    }

    @Override
    public String commandDesc() {
        return "Update ratelimit to cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("c", "clusterName", true, "update ratelimit to which cluster");
        optionGroup.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "update ratelimit to which broker");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("r", "ratelimitName", true, "the name of ratelimit to update.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "entityType", true, "the entityType of ratelimit rule: ip/channel_id/user/topic");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "entityName", true, "the entityName of ratelimit rule to update");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "matcherType", true, "the matcherType of ratelimit rule: in/startswith/endswith/cidr/any");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "putTps", true, "the produce TPS of ratelimit rule to update");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "getTps", true, "the consume TPS of ratelimit rule to update");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String name = StringUtils.trim(commandLine.getOptionValue('r'));
            String entityType = StringUtils.trim(commandLine.getOptionValue('t'));
            String entityName = StringUtils.trim(commandLine.getOptionValue('e'));
            String matcherType = StringUtils.trim(commandLine.getOptionValue('m'));
            double putTps = Double.parseDouble(StringUtils.trim(commandLine.getOptionValue('p')));
            double getTps = Double.parseDouble(StringUtils.trim(commandLine.getOptionValue('g')));

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.updateRatelimit(addr, new RatelimitInfo(name, entityType, entityName, matcherType, putTps, getTps));

                System.out.printf("update ratelimit to %s success.%n", addr);
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> brokerAddrSet =
                    CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : brokerAddrSet) {
                    defaultMQAdminExt.updateRatelimit(addr, new RatelimitInfo(name, entityType, entityName, matcherType, putTps, getTps));
                    System.out.printf("update ratelimit to %s success.%n", addr);
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
