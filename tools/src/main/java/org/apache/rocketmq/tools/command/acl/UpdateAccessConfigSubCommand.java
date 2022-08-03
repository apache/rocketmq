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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateAccessConfigSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateAclConfig";
    }

    @Override
    public String commandDesc() {
        return "Update acl config yaml file in broker";
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

        opt = new Option("s", "secretKey", true, "set secretKey in acl config file");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("w", "whiteRemoteAddress", true, "set white ip Address for account in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "defaultTopicPerm", true, "set default topicPerm in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "defaultGroupPerm", true, "set default GroupPerm in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topicPerms", true, "set topicPerms list,eg: topicA=DENY,topicD=SUB");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupPerms", true, "set groupPerms list,eg: groupD=DENY,groupD=SUB");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "admin", true, "set admin flag in acl config file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "namespace", true, "set namespace corresponding to accessKey topic and group");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        System.out.printf("updateAclConfig command will be deprecated. If you want to update secretKey whiteRemoteAddress " +
            "admin defaultTopicPerm and defaultGroupPerm, updateAclAccount command is recommended. If you want to update resource perm," +
            "updateAclResourcePerms command is recommended. If you want to update namespace perm, updateAclNamespacePerms command" +
            "is recommended.");
    }
}