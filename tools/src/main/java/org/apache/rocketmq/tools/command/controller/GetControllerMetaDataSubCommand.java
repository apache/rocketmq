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
package org.apache.rocketmq.tools.command.controller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetControllerMetaDataSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getControllerMetaData";
    }

    @Override
    public String commandDesc() {
        return "Get controller cluster's metadata.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "controllerAddress", true, "the address of controller");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        String controllerAddress = commandLine.getOptionValue('a').trim();
        try {
            defaultMQAdminExt.start();
            final GetMetaDataResponseHeader metaData = defaultMQAdminExt.getControllerMetaData(controllerAddress);
            System.out.printf("\n#ControllerGroup\t%s", metaData.getGroup());
            System.out.printf("\n#ControllerLeaderId\t%s", metaData.getControllerLeaderId());
            System.out.printf("\n#ControllerLeaderAddress\t%s", metaData.getControllerLeaderAddress());
            final String peers = metaData.getPeers();
            if (StringUtils.isNotEmpty(peers)) {
                final String[] peerList = peers.split(";");
                for (String peer : peerList) {
                    System.out.printf("\n#Peer:\t%s", peer);
                }
            }
            System.out.printf("\n");
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
