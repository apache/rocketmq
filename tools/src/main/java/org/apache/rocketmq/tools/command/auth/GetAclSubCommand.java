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
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetAclSubCommand implements SubCommand {

    private static final String FORMAT = "%-16s  %-22s  %-22s  %-22s  %-22s  %-22s%n";

    @Override
    public String commandName() {
        return "getAcl";
    }

    @Override
    public String commandDesc() {
        return "Get acl from cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "get acl for which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "get acl for specified cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("s", "subject", true, "the subject of acl to get");
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String subject = commandLine.getOptionValue('s').trim();

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();

                AclInfo aclInfo = defaultMQAdminExt.getAcl(addr, subject);
                if (aclInfo != null) {
                    printAcl(aclInfo);
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
                    AclInfo aclInfo = defaultMQAdminExt.getAcl(masterAddr, subject);
                    if (aclInfo != null) {
                        printAcl(aclInfo);
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

    private void printAcl(AclInfo acl) {
        if (acl == null) {
            return;
        }
        System.out.printf(FORMAT, "#Subject", "#PolicyType", "#Resource", "#Actions", "#SourceIp", "#Decision");
        List<AclInfo.PolicyInfo> policyInfos = acl.getPolicies();
        if (CollectionUtils.isEmpty(policyInfos)) {
            System.out.printf(FORMAT, acl.getSubject(), "", "", "", "", "");
        }
        policyInfos.forEach(policy -> {
            List<AclInfo.PolicyEntryInfo> entries = policy.getEntries();
            if (CollectionUtils.isEmpty(entries)) {
                return;
            }
            entries.forEach(entry -> {
                System.out.printf(FORMAT, acl.getSubject(), policy.getPolicyType(), entry.getResource(),
                    entry.getActions(), entry.getSourceIps(), entry.getDecision());
            });
        });
    }
}
