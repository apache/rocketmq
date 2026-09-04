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
package org.apache.rocketmq.tools.command.lite;

import com.alibaba.fastjson2.JSON;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.GetBrokerLiteInfoResponseBody;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetBrokerLiteInfoSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getBrokerLiteInfo";
    }

    @Override
    public String commandDesc() {
        return "Get broker lite info.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "Broker address");
        optionGroup.addOption(opt);

        opt = new Option("c", "cluster", true, "Cluster name");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("d", "showDetail", false, "Show topic and group detail info");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            boolean showDetail = commandLine.hasOption('d');

            printHeader();

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                GetBrokerLiteInfoResponseBody responseBody = defaultMQAdminExt.getBrokerLiteInfo(brokerAddr);
                printRow(responseBody, brokerAddr, showDetail);
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                Set<String> masterSet = CommandUtil
                    .fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String brokerAddr : masterSet) {
                    try {
                        GetBrokerLiteInfoResponseBody responseBody = defaultMQAdminExt.getBrokerLiteInfo(brokerAddr);
                        printRow(responseBody, brokerAddr, showDetail);
                    } catch (Exception e) {
                        System.out.printf("[%s] error.%n", brokerAddr);
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    static void printHeader() {
        System.out.printf("%-30s %-17s %-10s %-14s %-20s %-17s %-15s %-18s %-15s%n",
            "#Broker",
            "#Store Type",
            "#Max LMQ",
            "#Current LMQ",
            "#SubscriptionCount",
            "#OrderInfoCount",
            "#CQTableSize",
            "#OffsetTableSize",
            "#eventMapSize"
        );
    }

    static void printRow(
        GetBrokerLiteInfoResponseBody responseBody,
        String brokerAddr,
        boolean showDetail
    ) {
        System.out.printf("%-30s %-17s %-10s %-14s %-20s %-17s %-15s %-18s %-15s%n",
            brokerAddr,
            responseBody.getStoreType(),
            responseBody.getMaxLmqNum(),
            responseBody.getCurrentLmqNum(),
            responseBody.getLiteSubscriptionCount(),
            responseBody.getOrderInfoCount(),
            responseBody.getCqTableSize(),
            responseBody.getOffsetTableSize(),
            responseBody.getEventMapSize()
        );

        // If showDetail enabled, print Topic Meta and Group Meta on new lines
        if (showDetail) {
            System.out.printf("Topic Meta: %s%n", JSON.toJSONString(responseBody.getTopicMeta()));
            System.out.printf("Group Meta: %s%n%n", JSON.toJSONString(responseBody.getGroupMeta()));
        }
    }
}
