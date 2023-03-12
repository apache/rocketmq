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

import java.util.Arrays;

public class CleanControllerBrokerMetaSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "cleanBrokerMetadata";
    }

    @Override
    public String commandDesc() {
        return "Clean metadata of broker on controller";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        Option opt = new Option("a", "controllerAddress", true, "The address of controller");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerControllerIdsToClean", true, "The brokerController id list which requires to clean metadata. eg: 1;2;3, means that clean broker-1, broker-2 and broker-3");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("bn", "brokerName", true, "The broker name of the replicas that require to be manipulated");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "The clusterName of broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("l", "cleanLivingBroker", false, "Whether clean up living brokers,default value is false");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        String controllerAddress = commandLine.getOptionValue('a').trim();
        String brokerName = commandLine.getOptionValue("bn").trim();
        String clusterName = null;
        String brokerControllerIdsToClean = null;

        if (commandLine.hasOption('c')) {
            clusterName = commandLine.getOptionValue('c').trim();
        }
        if (commandLine.hasOption('b')) {
            brokerControllerIdsToClean = commandLine.getOptionValue('b').trim();
            try {
                Arrays.stream(brokerControllerIdsToClean.split(";")).map(idStr -> Long.parseLong(idStr));
            } catch (NumberFormatException numberFormatException) {
                throw new IllegalArgumentException("please set the option <brokerControllerIdsToClean> according to the format", numberFormatException);
            }
        }
        boolean isCleanLivingBroker = false;
        if (commandLine.hasOption('l')) {
            isCleanLivingBroker = true;
        }

        if (!isCleanLivingBroker && StringUtils.isEmpty(clusterName)) {
            throw new IllegalArgumentException("cleanLivingBroker option is false, clusterName option can not be empty.");
        }

        try {
            defaultMQAdminExt.start();
            defaultMQAdminExt.cleanControllerBrokerData(controllerAddress, clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);
            System.out.printf("clear broker %s metadata from controller success! \n", brokerName);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
