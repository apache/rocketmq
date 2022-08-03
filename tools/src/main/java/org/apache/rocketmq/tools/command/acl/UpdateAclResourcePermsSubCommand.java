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

package org.apache.rocketmq.tools.command.acl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.OperationType;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.ResourceAndPerm;
import org.apache.rocketmq.common.ResourceType;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateAclResourcePermsSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateAclResourcePerms";
    }

    @Override
    public String commandDesc() {
        return "Update ak's resourcePerms in broker";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "update acl config file to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "update acl config file to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("a", "accessKey", true, "set accessKey in acl config file");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "operation", true, "set operation type on resourcePerms");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "resource", true, "set the topic name or consumerGroup name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "type", true, "set the type of resource,eg: TOPIC or GROUP");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("m", "namespace", true, "set the namespace of the resource");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set the perm of the resource in the namespace");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        String operation = new String();
        ResourceAndPerm resourceAndPerm = new ResourceAndPerm();
        List<ResourceAndPerm> resourcePerms = new ArrayList<>(1);

        try {
            PlainAccessConfig accessConfig = new PlainAccessConfig();
            //accessKey
            accessConfig.setAccessKey(commandLine.getOptionValue('a').trim());

            //Operation
            if (commandLine.getOptionValue('o').trim().equalsIgnoreCase(OperationType.ADD.toString())) {
                operation = OperationType.ADD.toString();
            } else if (commandLine.getOptionValue('o').trim().equalsIgnoreCase(OperationType.UPDATE.toString())) {
                operation = OperationType.UPDATE.toString();
            } else if (commandLine.getOptionValue('o').trim().equalsIgnoreCase(OperationType.UPDATE.toString())) {
                operation = OperationType.DELETE.toString();
            }


            //Resource
            resourceAndPerm.setResource(commandLine.getOptionValue('r').trim());

            //Type
            if (commandLine.getOptionValue('t').trim().equalsIgnoreCase(ResourceType.GROUP.toString())) {
                resourceAndPerm.setType(ResourceType.GROUP);
            } else if (commandLine.getOptionValue('t').trim().equalsIgnoreCase(ResourceType.TOPIC.toString())) {
                resourceAndPerm.setType(ResourceType.TOPIC);
            }

            //Namespace
            if (commandLine.hasOption('m')) {
                resourceAndPerm.setNamespace(commandLine.getOptionValue('m').trim());
            }

            //Perm
            if (commandLine.hasOption('p')) {
                resourceAndPerm.setPerm(commandLine.getOptionValue('p').trim());
            }

            resourcePerms.add(resourceAndPerm);
            accessConfig.setResourcePerms(resourcePerms);

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();

                defaultMQAdminExt.updateAclResourcePerms(addr, accessConfig, operation);

                System.out.printf("add or udpate or delete acl resource perms to %s success.%n", addr);
                System.out.printf("%s", accessConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> brokerAddrSet =
                    CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : brokerAddrSet) {
                    defaultMQAdminExt.updateAclResourcePerms(addr, accessConfig, operation);
                    System.out.printf("add or udpate or delete acl resource perms to %s success.%n", addr);
                }

                System.out.printf("%s", accessConfig);
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
