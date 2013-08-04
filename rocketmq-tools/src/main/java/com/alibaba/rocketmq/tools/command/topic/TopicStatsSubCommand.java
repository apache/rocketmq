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
package com.alibaba.rocketmq.tools.command.topic;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.admin.TopicOffset;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 查看Topic统计信息，包括offset、最后更新时间
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-3
 */
public class TopicStatsSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicStats";
    }


    @Override
    public String commandDesc() {
        return "Examine topic stats info";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            if (!commandLine.hasOption('t')) {
                MixAll.printCommandLineHelp("mqadmin " + this.commandName(), options);
                return;
            }

            defaultMQAdminExt.start();

            String topic = commandLine.getOptionValue('t');
            TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topic);

            Iterator<Map.Entry<MessageQueue, TopicOffset>> it =
                    topicStatsTable.getOffsetTable().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, TopicOffset> next = it.next();
                MessageQueue mq = next.getKey();
                TopicOffset to = next.getValue();

                System.out.printf("%32s %4d %20d %20d %20d\n",//
                    mq.getBrokerName(),//
                    mq.getQueueId(),//
                    to.getMinOffset(),//
                    to.getMaxOffset(),//
                    to.getLastUpdateTimestamp()//
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
