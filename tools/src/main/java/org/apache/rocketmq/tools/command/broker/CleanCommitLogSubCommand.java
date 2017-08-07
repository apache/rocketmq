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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CleanCommitLogSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "cleanCommitLog";
    }

    @Override
    public String commandDesc() {
        return "Clean commit log files";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option clusterOption = new Option("c", "clusterName", true, "Cluster name");
        options.addOption(clusterOption);

        Option brokerOption = new Option("b", "brokerAddress", true, "Broker address");
        options.addOption(brokerOption);

        Option watermarkOption = new Option("w", "watermark", true, "Watermark level in percent");
        options.addOption(watermarkOption);

        Option forceOption = new Option("f", "force", false, "Force to clean commit log to meet watermark or not");
        options.addOption(forceOption);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String brokerAddress = commandLine.getOptionValue("b");
            String clusterName = commandLine.getOptionValue("c");
            int watermark = commandLine.hasOption("w") ? Integer.parseInt(commandLine.getOptionValue("w")) : 100;
            boolean force = commandLine.hasOption("f");

            if (null != brokerAddress && !brokerAddress.isEmpty()) {
                try {
                    defaultMQAdminExt.cleanCommitLog(brokerAddress, watermark, force);
                } catch (InterruptedException | RemotingTimeoutException | RemotingConnectException | RemotingSendRequestException e) {
                    throw new SubCommandException("Failed to clean commit log", e);
                }
            } else if (null != clusterName && !clusterName.isEmpty()) {
                List<String> failedList = new ArrayList<>();
                try {
                    Set<String> masterSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                    for (String address : masterSet) {
                        try {
                            defaultMQAdminExt.cleanCommitLog(address, watermark, force);
                        } catch (Exception e) {
                            failedList.add(address);
                        }
                    }
                } catch (InterruptedException | RemotingConnectException | RemotingTimeoutException | RemotingSendRequestException | MQBrokerException e) {
                    throw new SubCommandException("Failed to figure out addresses of brokers by cluster name", e);
                }

                if (!failedList.isEmpty()) {
                    throw new SubCommandException("Failed to clean commit log files for some brokers: " + failedList.toString());
                }
            } else {
                // print help
                throw new SubCommandException("Either brokerAddress or clusterName is required");
            }

        } catch (MQClientException e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
