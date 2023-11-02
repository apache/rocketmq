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


import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CommitLogSetReadAheadSubCommand implements SubCommand {
    private static final String MADV_RANDOM = "1";
    private static final String MADV_NORMAL = "0";
    @Override
    public String commandName() {
        return "setCommitLogReadAheadMode";
    }

    @Override
    public String commandDesc() {
        return "Set read ahead mode for all commitlog files.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "set which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "set which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "commitLogReadAheadMode", true, "set the CommitLog read ahead mode; 0 is default, 1 random read");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String mode = commandLine.getOptionValue('m').trim();
            if (!mode.equals(MADV_RANDOM) && !mode.equals(MADV_NORMAL)) {
                System.out.printf("set the read mode error; 0 is default, 1 random read\n");
                return;
            }
            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();
                setAndPrint(defaultMQAdminExt, String.format("============%s============\n", brokerAddr), brokerAddr, mode);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();
                Map<String, List<String>> masterAndSlaveMap = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);
                for (String masterAddr : masterAndSlaveMap.keySet()) {
                    setAndPrint(defaultMQAdminExt, String.format("============Master: %s============\n", masterAddr), masterAddr, mode);
                    for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {
                        setAndPrint(defaultMQAdminExt, String.format("============My Master: %s=====Slave: %s============\n", masterAddr, slaveAddr), slaveAddr, mode);
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected void setAndPrint(final MQAdminExt defaultMQAdminExt, final String printPrefix, final String addr, final String mode)
        throws InterruptedException, RemotingConnectException, UnsupportedEncodingException, RemotingTimeoutException, MQBrokerException, RemotingSendRequestException {
        System.out.print(" " + printPrefix);
        System.out.printf("commitLog set readAhead mode rstStr" + defaultMQAdminExt.setCommitLogReadAheadMode(addr, mode) + "\n");
    }
}