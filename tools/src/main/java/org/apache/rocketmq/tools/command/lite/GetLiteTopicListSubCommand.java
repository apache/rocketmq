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
import org.apache.rocketmq.remoting.protocol.body.GetLiteTopicListResponseBody;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetLiteTopicListSubCommand implements SubCommand {

    private static final int DEFAULT_MAX_COUNT = 1000;

    @Override
    public String commandName() {
        return "getLiteTopicList";
    }

    /**
     * The command is also exposed as {@code listLiteTopic}, which reads more naturally for an
     * enumeration command while keeping the {@code getLite*} naming used by the rest of the lite
     * admin commands as the canonical name.
     */
    @Override
    public String commandAlias() {
        return "listLiteTopic";
    }

    @Override
    public String commandDesc() {
        return "List lite topics under a parent topic.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("p", "parentTopic", true, "Parent topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("x", "prefix", true, "Lite topic name prefix");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "maxCount", true, "Maximum number of lite topics to return");
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
            String prefix = commandLine.hasOption('x') ? commandLine.getOptionValue('x').trim() : null;
            int maxCount = commandLine.hasOption('m')
                ? Integer.parseInt(commandLine.getOptionValue('m').trim()) : DEFAULT_MAX_COUNT;
            if (maxCount <= 0) {
                throw new IllegalArgumentException("maxCount must be greater than 0");
            }

            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(parentTopic);
            System.out.printf("Lite Topic List: [%s]%s%n", parentTopic,
                prefix == null || prefix.isEmpty() ? "" : " prefix=[" + prefix + "]");
            printHeader();

            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                String brokerAddr = brokerData.selectBrokerAddr();
                if (null == brokerAddr) {
                    continue;
                }
                try {
                    GetLiteTopicListResponseBody body = defaultMQAdminExt
                        .getLiteTopicList(brokerAddr, parentTopic, prefix, maxCount);
                    printRow(body, brokerData.getBrokerName());
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
        System.out.printf("%-30s %-40s %-14s %-14s %-25s %-12s %-12s %n",
            "#Broker Name",
            "#LiteTopic",
            "#MinOffset",
            "#MaxOffset",
            "#LastUpdate",
            "#Sharding",
            "#SubCount"
        );
    }

    static void printRow(GetLiteTopicListResponseBody responseBody, String brokerName) {
        if (responseBody == null || responseBody.getLiteTopicList() == null) {
            return;
        }
        for (GetLiteTopicListResponseBody.LiteTopicInfo info : responseBody.getLiteTopicList()) {
            System.out.printf("%-30s %-40s %-14s %-14s %-25s %-12s %-12s %n",
                UtilAll.frontStringAtLeast(brokerName, 30),
                UtilAll.frontStringAtLeast(info.getLiteTopic(), 40),
                info.getMinOffset(),
                info.getMaxOffset(),
                info.getLastUpdateTimestamp() > 0
                    ? UtilAll.timeMillisToHumanString2(info.getLastUpdateTimestamp()) : "-",
                info.isShardingToBroker(),
                info.getSubscriberCount()
            );
        }
        if (responseBody.isHasMore()) {
            System.out.printf("[%s] returned %d lite topics; increase -m to list more.%n",
                brokerName, responseBody.getTotal());
        }
    }
}
