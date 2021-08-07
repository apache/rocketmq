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

package org.apache.rocketmq.tools.command.topic;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicQueueStatistics;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TopicStatisticsSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicStatistics";
    }

    @Override
    public String commandDesc() {
        return "Examine topic Statistic info";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
                        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue('t').trim();
            TopicStatsTable<TopicQueueStatistics> topicStatsTable = defaultMQAdminExt.examineTopicStatistics(topic);

            List<MessageQueue> mqList = new LinkedList<MessageQueue>();
            mqList.addAll(topicStatsTable.getOffsetTable().keySet());
            Collections.sort(mqList);

            System.out.printf("%-32s  %-4s  %-20s  %-20s  %-20s  %-20s  %s%n",
                    "#Broker Name",
                    "#QID",
                    "#Min Offset",
                    "#Max Offset",
                    "#Message Count Total",
                    "#Message Size Total",
                    "#Last Updated"
            );

            for (MessageQueue mq : mqList) {
                TopicQueueStatistics topicQueueStatistics = topicStatsTable.getOffsetTable().get(mq);

                String humanTimestamp = "";
                if (topicQueueStatistics.getLastUpdateTimestamp() > 0) {
                    humanTimestamp = UtilAll.timeMillisToHumanString2(topicQueueStatistics.getLastUpdateTimestamp());
                }

                System.out.printf("%-32s  %-4d  %-20d  %-20d  %-20d  %-20d  %s%n",
                        UtilAll.frontStringAtLeast(mq.getBrokerName(), 32),
                        mq.getQueueId(),
                        topicQueueStatistics.getMinOffset(),
                        topicQueueStatistics.getMaxOffset(),
                        topicQueueStatistics.getMessageCountTotal(),
                        topicQueueStatistics.getMessageSizeTotal(),
                        humanTimestamp
                );
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
