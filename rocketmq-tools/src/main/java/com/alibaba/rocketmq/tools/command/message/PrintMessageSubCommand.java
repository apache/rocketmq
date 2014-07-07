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
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 打印指定Topic的所有消息，某个时间区间，方便排查问题
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2014-6-22
 */
public class PrintMessageSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "printMsg";
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

        opt = new Option("s", "subExpression ", true, "Subscribe Expression(eg: TagA || TagB)");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
                new Option("b", "beginTimestamp ", true,
                    "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
                new Option("e", "endTimestamp ", true,
                    "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
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


    public static long timestampFormat(final String value) {
        long timestamp = 0;
        try {
            // 直接输入 long 类型的 timestamp
            timestamp = Long.valueOf(value);
        }
        catch (NumberFormatException e) {
            // 输入的为日期格式，精确到毫秒
            timestamp = UtilAll.parseDate(value, UtilAll.yyyy_MM_dd_HH_mm_ss_SSS).getTime();
        }

        return timestamp;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, rpcHook);

        try {
            String topic = commandLine.getOptionValue('t').trim();

            String charsetName = //
                    !commandLine.hasOption('c') ? "UTF-8" : commandLine.getOptionValue('c').trim();

            String subExpression = //
                    !commandLine.hasOption('s') ? "*" : commandLine.getOptionValue('s').trim();

            consumer.start();

            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            for (MessageQueue mq : mqs) {
                long minOffset = consumer.minOffset(mq);
                long maxOffset = consumer.maxOffset(mq);

                if (commandLine.hasOption('b')) {
                    String timestampStr = commandLine.getOptionValue('b').trim();
                    long timeValue = timestampFormat(timestampStr);
                    minOffset = consumer.searchOffset(mq, timeValue);
                }

                if (commandLine.hasOption('e')) {
                    String timestampStr = commandLine.getOptionValue('e').trim();
                    long timeValue = timestampFormat(timestampStr);
                    maxOffset = consumer.searchOffset(mq, timeValue);
                }

                READQ: for (long offset = minOffset; offset < maxOffset;) {
                    try {
                        PullResult pullResult = consumer.pull(mq, subExpression, offset, 32);
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
        }
    }
}
