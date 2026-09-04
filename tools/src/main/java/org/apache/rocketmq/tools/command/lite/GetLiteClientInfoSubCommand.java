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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.GetLiteClientInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetLiteClientInfoSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getLiteClientInfo";
    }

    @Override
    public String commandDesc() {
        return "Get lite client info.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("p", "parentTopic", true, "Parent topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "group", true, "Consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clientId", true, "Client id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "showDetail", false, "Show details");
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

            String parentTopic = commandLine.getOptionValue('p').trim();
            String group = commandLine.getOptionValue('g').trim();
            String clientId = commandLine.getOptionValue('c').trim();
            boolean showLiteTopic = commandLine.hasOption('s');

            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(parentTopic);
            System.out.printf("Lite Client Info: [%s] [%s] [%s]%n", parentTopic, group, clientId);

            printHeader();

            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                String brokerAddr = brokerData.selectBrokerAddr();
                String brokerName = brokerData.getBrokerName();
                if (null == brokerAddr) {
                    continue;
                }
                try {
                    GetLiteClientInfoResponseBody body = defaultMQAdminExt
                        .getLiteClientInfo(brokerAddr, parentTopic, group, clientId);
                    printRow(body, brokerName, showLiteTopic);
                } catch (Exception e) {
                    System.out.printf("[%s] error.%n", brokerData.getBrokerName());
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    static void printHeader() {
        System.out.printf("%-30s %-20s %-30s %-30s %n",
            "#Broker",
            "#LiteTopicCount",
            "#LastAccessTime",
            "#LastConsumeTime"
        );
    }

    static void printRow(
        GetLiteClientInfoResponseBody responseBody,
        String brokerName,
        boolean showDetail
    ) {
        System.out.printf("%-30s %-20s %-30s %-30s %n",
            brokerName,
            responseBody.getLiteTopicCount() > 0 ? responseBody.getLiteTopicCount() : "N/A",
            responseBody.getLastAccessTime() > 0
                ? UtilAll.timeMillisToHumanString2(responseBody.getLastAccessTime()) : "N/A",
            responseBody.getLastConsumeTime() > 0
                ? UtilAll.timeMillisToHumanString2(responseBody.getLastConsumeTime()) : "N/A"
        );

        if (showDetail && responseBody.getLiteTopicSet() != null) {
            System.out.printf("Lite Topics: %s%n%n", responseBody.getLiteTopicSet());
        }
    }
}
