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
package org.apache.rocketmq.tools.command.auth;

import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ListUserSubCommand implements SubCommand {

    private static final String FORMAT = "%-16s  %-22s  %-22s%n";

    @Override
    public String commandName() {
        return "getUser";
    }

    @Override
    public String commandDesc() {
        return "Get user from cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "get user for which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "get user for specified cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("f", "filter", true, "the filter to list users");
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String filter = commandLine.getOptionValue('f').trim();

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();

                List<UserInfo> userInfos = defaultMQAdminExt.listUser(addr, filter);
                if (CollectionUtils.isEmpty(userInfos)) {
                    printUsers(userInfos);
                }

                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                if (CollectionUtils.isEmpty(masterSet)) {
                    throw new SubCommandException(this.getClass().getSimpleName() + " command failed, there is no broker in cluster.");
                }
                for (String masterAddr : masterSet) {
                    List<UserInfo> userInfos = defaultMQAdminExt.listUser(masterAddr, filter);
                    if (CollectionUtils.isEmpty(userInfos)) {
                        printUsers(userInfos);
                        System.out.printf("get user from %s success.%n", masterAddr);
                        break;
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

    private void printUsers(List<UserInfo> users) {
        if (CollectionUtils.isEmpty(users)) {
            return;
        }
        System.out.printf(FORMAT, "#UserName", "#Password", "#UserType");
        users.forEach(user -> System.out.printf(FORMAT, user.getUsername(), user.getPassword(), user.getUserType()));
    }
}
