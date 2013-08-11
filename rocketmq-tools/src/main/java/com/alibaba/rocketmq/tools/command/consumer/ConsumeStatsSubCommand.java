/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.consumer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 查看订阅组消费状态，消费进度
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-11
 */
public class ConsumeStatsSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumeStats";
    }


    @Override
    public String commandDesc() {
        return "Consume Stats, Progress, Speed";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String consumerGroup = commandLine.getOptionValue('g');
            ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);

            List<MessageQueue> mqList = new LinkedList<MessageQueue>();
            mqList.addAll(consumeStats.getOffsetTable().keySet());
            Collections.sort(mqList);

            for (MessageQueue mq : mqList) {
                OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);

                long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();

                System.out.printf("%-32s  %-32s  %4d  %20d  %20d  %d\n",//
                    mq.getTopic(),//
                    mq.getBrokerName(),//
                    mq.getQueueId(),//
                    offsetWrapper.getBrokerOffset(),//
                    offsetWrapper.getConsumerOffset(),//
                    diff //
                    );
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
