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
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CleanControllerBrokerDataSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "cleanBrokerData";
    }

    @Override
    public String commandDesc() {
        return "Clean data of broker on controller";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        Option opt = new Option("a", "controllerAddress", true, "The address of controller");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerAddress", true, "The address of the broker which requires to clean metadata. eg: 192.168.0.1:30911;192.168.0.2:30911");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("n", "brokerName", true, "The broker name of the replicas that require to be manipulated");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "the clusterName of broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("l", "cleanLivingBroker", false, " whether clean up living brokers,default value is false");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        String controllerAddress = commandLine.getOptionValue('a').trim();
        String brokerName = commandLine.getOptionValue('n').trim();
        String clusterName = null;
        String brokerAddress = null;

        if (commandLine.hasOption('c')) {
            clusterName = commandLine.getOptionValue('c').trim();
        }
        if (commandLine.hasOption('b')) {
            brokerAddress = commandLine.getOptionValue('b').trim();
        }
        boolean isCleanLivingBroker = false;
        if (commandLine.hasOption('l')) {
            isCleanLivingBroker = true;
        }

        if (!isCleanLivingBroker && StringUtils.isEmpty(clusterName)) {
            throw new IllegalArgumentException("cleanLivingBroker option is false,clusterName option can not be empty.");
        }

        try {
            defaultMQAdminExt.start();
            defaultMQAdminExt.cleanControllerBrokerData(controllerAddress, clusterName, brokerName, brokerAddress, isCleanLivingBroker);
            System.out.printf("clear broker %s data from controller success! \n", brokerName);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
