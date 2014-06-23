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

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 打印指定Topic的所有消息，某个时间区间，方便排查问题
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-12
 */
public class PrintMessageSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "printMessage";
    }


    @Override
    public String commandDesc() {
        return "Print Message Detail";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "charsetName ", true, "CharsetName(eg: UTF-8、GBK)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    public static void printMessage(final List<MessageExt> msgs, final String charsetName) {
        for (MessageExt msg : msgs) {
            try {
                System.out.printf("MSGID: %s %s BODY: %s\n", msg.getMsgId(), msg.toString(),
                    new String(msg.getBody(), charsetName));
            }
            catch (UnsupportedEncodingException e) {
            }
        }
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP);

        try {
            String topic = commandLine.getOptionValue('t').trim();
            String charsetName = commandLine.getOptionValue('c');
            if (null == charsetName) {
                charsetName = "UTF-8";
            }
            charsetName.trim();

            consumer.start();
            adminExt.start();

            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            for (MessageQueue mq : mqs) {
                long minOffset = consumer.minOffset(mq);
                long maxOffset = consumer.maxOffset(mq);
                READQ: for (long offset = minOffset; offset < maxOffset;) {
                    try {
                        PullResult pullResult = consumer.pull(mq, "*", offset, 32);
                        offset = pullResult.getNextBeginOffset();
                        switch (pullResult.getPullStatus()) {
                        case FOUND:
                            printMessage(pullResult.getMsgFoundList(), charsetName);
                            break;
                        case NO_MATCHED_MSG:
                        case NO_NEW_MSG:
                        case OFFSET_ILLEGAL:
                            break READQ;
                        }
                    }
                    catch (Exception e) {
                        break;
                    }
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            consumer.shutdown();
            adminExt.shutdown();
        }
    }
}
