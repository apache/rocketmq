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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 修改、创建Topic配置命令
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class UpdateTopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopic";
    }


    @Override
    public String commandDesc() {
        return "Update or create topic";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "readQueueNums", true, "set read queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "writeQueueNums", true, "set write queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(W|R|WR)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String addr = null;
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);

            // brokerAddr
            if (commandLine.hasOption('b')) {
                addr = commandLine.getOptionValue('b');
            }
            else {
                System.out.println("please tell us broker's addr");
                return;
            }

            // topic
            if (commandLine.hasOption('t')) {
                topicConfig.setTopicName(commandLine.getOptionValue('t'));
            }
            else {
                System.out.println("please tell us topic name");
                return;
            }

            // readQueueNums
            if (commandLine.hasOption('r')) {
                topicConfig.setReadQueueNums(Integer.parseInt(commandLine.getOptionValue('r')));
            }

            // writeQueueNums
            if (commandLine.hasOption('w')) {
                topicConfig.setWriteQueueNums(Integer.parseInt(commandLine.getOptionValue('w')));
            }

            // perm
            if (commandLine.hasOption('p')) {
                topicConfig.setPerm(Integer.parseInt(commandLine.getOptionValue('p')));
            }

            defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
            System.out.println("create topic success.");
            System.out.println(topicConfig);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
