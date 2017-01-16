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
package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;

public class AddCommitLogStorePathSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "addCommitLogStorePath";
    }

    @Override
    public String commandDesc() {
        return "Add store path for commit log";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option option = new Option("p", "path", true, "Commit log path");
        option.setRequired(true);
        options.addOption(option);

        Option brokerAddressOption = new Option("b", "brokerAddress", true, "Broker address");
        brokerAddressOption.setRequired(true);
        options.addOption(brokerAddressOption);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(String.valueOf(System.currentTimeMillis()));

        String brokerAddress = null;

        if (commandLine.hasOption("b")) {
            brokerAddress = commandLine.getOptionValue("b");
        } else {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("Broker address missing", options);
            return;
        }

        String storePathCommitLog = null;
        if (commandLine.hasOption("p")) {
            storePathCommitLog = commandLine.getOptionValue("p");
        } else {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("storePathCommitLog missing", options);
            return;
        }

        try {
            adminExt.start();
            adminExt.addCommitLogStorePath(brokerAddress, storePathCommitLog);
            System.out.printf("Commit log store path added OK");
        } catch (MQClientException | InterruptedException | MQBrokerException | RemotingException e) {
            e.printStackTrace();
        } finally {
            adminExt.shutdown();
        }
    }
}
