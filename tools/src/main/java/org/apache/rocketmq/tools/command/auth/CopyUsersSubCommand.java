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
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CopyUsersSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "copyUser";
    }

    @Override
    public String commandDesc() {
        return "Copy user to cluster.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        Option opt = new Option("f", "fromBroker", true, "the source broker that the users copy from");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "toBroker", true, "the target broker that the users copy to");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("u", "usernames", true, "the username list of user to copy.");
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
                String usernames = StringUtils.trim(commandLine.getOptionValue('u'));

                defaultMQAdminExt.start();

                List<UserInfo> userInfos = new ArrayList<>();
                if (StringUtils.isNotBlank(usernames)) {
                    for (String username : StringUtils.split(usernames, ",")) {
                        UserInfo userInfo = defaultMQAdminExt.getUser(sourceBroker, username);
                        if (userInfo != null) {
                            userInfos.add(userInfo);
                        }
                    }
                } else {
                    userInfos = defaultMQAdminExt.listUser(sourceBroker, null);
                }

                if (CollectionUtils.isEmpty(userInfos)) {
                    return;
                }

                for (UserInfo userInfo : userInfos) {
                    if (defaultMQAdminExt.getUser(targetBroker, userInfo.getUsername()) == null) {
                        defaultMQAdminExt.createUser(targetBroker, userInfo);
                    } else {
                        defaultMQAdminExt.updateUser(targetBroker, userInfo);
                    }
                    System.out.printf("copy user of %s from %s to %s success.%n", userInfo.getUsername(), sourceBroker, targetBroker);
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
