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
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilALl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据消息Id或者消息Key查询消息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-12
 */
public class QueryMessageSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryMessage";
    }


    @Override
    public String commandDesc() {
        return "Query Message by Id or Key";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("k", "msgKey", true, "Message Key");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "fallbackHours", true, "Fallback Hours");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    void queryById(final DefaultMQAdminExt admin, final String msgId) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException {
        admin.start();
        MessageExt msg = admin.viewMessage(msgId);

        System.out.printf("%-20s %s\n",//
            "Topic:",//
            msg.getTopic()//
            );

        System.out.printf("%-20s %d\n",//
            "Queue ID:",//
            msg.getQueueId()//
            );

        System.out.printf("%-20s %d\n",//
            "Queue Offset",//
            msg.getQueueOffset()//
            );

        System.out.printf("%-20s %d\n",//
            "CommitLog Offset",//
            msg.getCommitLogOffset()//
            );

        System.out.printf("%-20s %s\n",//
            "Born Timestamp",//
            UtilALl.timeMillisToHumanString2(msg.getBornTimestamp())//
            );

        System.out.printf("%-20s %s\n",//
            "Store Timestamp",//
            UtilALl.timeMillisToHumanString2(msg.getStoreTimestamp())//
            );

        System.out.printf("%-20s %s\n",//
            "Born Host",//
            RemotingHelper.parseSocketAddressAddr(msg.getBornHost())//
            );

        System.out.printf("%-20s %s\n",//
            "Store Host",//
            RemotingHelper.parseSocketAddressAddr(msg.getStoreHost())//
            );
    }


    void queryByKey(final DefaultMQAdminExt admin, final String topic, final String key,
            final long fallbackHours) throws MQClientException, InterruptedException {
        admin.start();

        long end = System.currentTimeMillis() - (fallbackHours * 60 * 60 * 1000);
        long begin = end - (6 * 60 * 60 * 1000);

        QueryResult queryResult = admin.queryMessage(topic, key, 32, begin, end);
        for (MessageExt msg : queryResult.getMessageList()) {
            System.out.printf("%-50s %-4d %d\n", msg.getMsgId(), msg.getQueueId(), msg.getQueueOffset());
        }
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            if (commandLine.hasOption('i')) {
                final String msgId = commandLine.getOptionValue('i');
                this.queryById(defaultMQAdminExt, msgId);
            }
            else if (commandLine.hasOption('k') && commandLine.hasOption('t')) {
                final String topic = commandLine.getOptionValue('t');
                final String key = commandLine.getOptionValue('k');
                final String fallbackHours = commandLine.getOptionValue('f');
                long h = 0;
                if (fallbackHours != null) {
                    h = Long.parseLong(fallbackHours);
                }
                this.queryByKey(defaultMQAdminExt, topic, key, h);
            }
            else {
                MixAll.printCommandLineHelp("mqadmin " + this.commandName(), options);
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
