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
package com.alibaba.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据消息Key查询消息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-12
 */
public class QueryMsgByKeySubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryMsgByKey";
    }


    @Override
    public String commandDesc() {
        return "Query Message by Key";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "msgKey", true, "Message Key");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "fallbackHours", true, "Fallback Hours");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    void queryByKey(final DefaultMQAdminExt admin, final String topic, final String key,
            final long fallbackHours) throws MQClientException, InterruptedException {
        admin.start();

        /**
         * 默认查询三天的前到现在的消息
         */
        long end = System.currentTimeMillis() - (fallbackHours * 60 * 60 * 1000);
        long begin = end - (72 * 60 * 60 * 1000);

        QueryResult queryResult = admin.queryMessage(topic, key, 32, begin, end);
        System.out.printf("%-50s %-4s  %s\n",//
            "#Message ID",//
            "#QID",//
            "#Offset");
        for (MessageExt msg : queryResult.getMessageList()) {
            System.out.printf("%-50s %-4d %d\n", msg.getMsgId(), msg.getQueueId(), msg.getQueueOffset());
        }
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            final String topic = commandLine.getOptionValue('t').trim();
            final String key = commandLine.getOptionValue('k').trim();
            long h = 0;
            if (commandLine.hasOption('f')) {
                final String fallbackHours = commandLine.getOptionValue('f').trim();
                if (fallbackHours != null) {
                    h = Long.parseLong(fallbackHours);
                }
            }

            this.queryByKey(defaultMQAdminExt, topic, key, h);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
