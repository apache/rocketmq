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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.MQAdminStartup;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CopyAclsSubCommand implements SubCommand {

    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "120.27.249.46:9876");
        MQAdminStartup.main(new String[]{new CopyAclsSubCommand().commandName(), "-f", "120.27.249.46:30911", "-t", "47.97.177.118:30911"});
    }

    @Override
    public String commandName() {
        return "copyAcl";
    }

    @Override
    public String commandDesc() {
        return "Copy acl to cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        Option opt = new Option("f", "fromBroker", true, "the source broker that the acls copy from");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "toBroker", true, "the target broker that the acls copy to");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "subjects", true, "the subject list of acl to copy.");
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
            if (commandLine.hasOption("f") && commandLine.hasOption("t")) {
                String sourceBroker = StringUtils.trim(commandLine.getOptionValue("f"));
                String targetBroker = StringUtils.trim(commandLine.getOptionValue("t"));
                String subjects = StringUtils.trim(commandLine.getOptionValue('s'));

                defaultMQAdminExt.start();

                List<AclInfo> aclInfos = new ArrayList<>();
                if (StringUtils.isNotBlank(subjects)) {
                    for (String subject : StringUtils.split(subjects, ",")) {
                        AclInfo aclInfo = defaultMQAdminExt.getAcl(sourceBroker, subject);
                        if (aclInfo != null) {
                            aclInfos.add(aclInfo);
                        }
                    }
                } else {
                    aclInfos = defaultMQAdminExt.listAcl(sourceBroker, null, null);
                }

                if (CollectionUtils.isEmpty(aclInfos)) {
                    return;
                }

                for (AclInfo aclInfo : aclInfos) {
                    if (defaultMQAdminExt.getAcl(targetBroker, aclInfo.getSubject()) == null) {
                        defaultMQAdminExt.createAcl(targetBroker, aclInfo);
                    } else {
                        defaultMQAdminExt.updateAcl(targetBroker, aclInfo);
                    }
                    System.out.printf("copy acl of %s from %s to %s success.%n", aclInfo.getSubject(), sourceBroker, targetBroker);
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
