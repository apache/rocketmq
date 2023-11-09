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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CreateAclSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "createAcl";
    }

    @Override
    public String commandDesc() {
        return "Create acl to cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("c", "clusterName", true, "update acl config file to which cluster");
        optionGroup.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "create acl to which broker");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("s", "subject", true, "the subject of acl to create.");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "resources", true, "the resources of acl to create");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("a", "actions", true, "the actions of acl to create");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("d", "decision", true, "the decision of acl to create");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "sourceIp", true, "the sourceIps of acl to create");
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            CreateAclRequestHeader requestHeader = new CreateAclRequestHeader();
            String subject = commandLine.getOptionValue('s').trim();
            List<String> resources = Arrays.asList(commandLine.getOptionValue('r').trim().split("[;,]"));
            List<String> actions = Arrays.asList(commandLine.getOptionValue('a').trim().split("[;,]"));
            List<String> sourceIps = null;
            if (StringUtils.isNotBlank(commandLine.getOptionValue('i'))) {
                sourceIps = Arrays.asList(commandLine.getOptionValue('i').trim().split("[;,]"));
            }
            String decision = commandLine.getOptionValue('d').trim();

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.createAcl(addr, subject, resources, actions, sourceIps, decision);

                System.out.printf("create acl to %s success.%n", addr);
                System.out.printf("%s", requestHeader);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> brokerAddrSet =
                    CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : brokerAddrSet) {
                    defaultMQAdminExt.createAcl(addr, subject, resources, actions, sourceIps, decision);
                    System.out.printf("create acl to %s success.%n", addr);
                }

                System.out.printf("%s", requestHeader);
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
