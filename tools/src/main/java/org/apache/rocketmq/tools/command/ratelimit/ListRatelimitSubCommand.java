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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.RatelimitInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.List;
import java.util.Set;

public class ListRatelimitSubCommand implements SubCommand {

    private static final String FORMAT = "%-16s  %-22s  %-22s  %-22s  %-22s  %-22s%n";

    @Override
    public String commandName() {
        return "listRatelimit";
    }

    @Override
    public String commandDesc() {
        return "List ratelimit from cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "list ratelimit for which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "list ratelimit for specified cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            if (commandLine.hasOption('b')) {
                String addr = StringUtils.trim(commandLine.getOptionValue('b'));
                defaultMQAdminExt.start();

                List<RatelimitInfo> ratelimitInfos = defaultMQAdminExt.listRatelimit(addr);
                if (CollectionUtils.isNotEmpty(ratelimitInfos)) {
                    printRatelimits(ratelimitInfos);
                }
                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = StringUtils.trim(commandLine.getOptionValue('c'));

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                if (CollectionUtils.isEmpty(masterSet)) {
                    throw new SubCommandException(this.getClass().getSimpleName() + " command failed, there is no broker in cluster.");
                }
                for (String masterAddr : masterSet) {
                    List<RatelimitInfo> ratelimitInfos = defaultMQAdminExt.listRatelimit(masterAddr);
                    System.out.printf("ratelimit rules in broker %s:%n", masterAddr);
                    if (CollectionUtils.isNotEmpty(ratelimitInfos)) {
                        printRatelimits(ratelimitInfos);
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

    private void printRatelimits(List<RatelimitInfo> ratelimits) {
        System.out.printf(FORMAT, "#Name", "#EntityType", "#MatcherType", "#EntityName", "#ProduceTps", "#ConsumeTps");
        ratelimits.forEach(ratelimit -> System.out.printf(FORMAT,
            ratelimit.getName(), ratelimit.getEntityType(), ratelimit.getMatcherType(), ratelimit.getEntityName(),
            ratelimit.getProduceTps(), ratelimit.getConsumeTps()));
    }
}
