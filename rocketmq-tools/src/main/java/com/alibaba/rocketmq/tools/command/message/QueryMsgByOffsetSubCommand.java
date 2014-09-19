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

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据消息Offset查询消息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-12
 */
public class QueryMsgByOffsetSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryMsgByOffset";
    }


    @Override
    public String commandDesc() {
        return "Query Message by offset";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerName", true, "Broker Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "queueId", true, "Queue Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "Queue Offset");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQPullConsumer.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String topic = commandLine.getOptionValue('t').trim();
            String brokerName = commandLine.getOptionValue('b').trim();
            String queueId = commandLine.getOptionValue('i').trim();
            String offset = commandLine.getOptionValue('o').trim();

            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(brokerName);
            mq.setQueueId(Integer.parseInt(queueId));

            defaultMQPullConsumer.start();
            defaultMQAdminExt.start();

            PullResult pullResult = defaultMQPullConsumer.pull(mq, "*", Long.parseLong(offset), 1);
            if (pullResult != null) {
                switch (pullResult.getPullStatus()) {
                case FOUND:
                    QueryMsgByIdSubCommand.queryById(defaultMQAdminExt, pullResult.getMsgFoundList().get(0)
                        .getMsgId());
                    break;
                case NO_MATCHED_MSG:
                case NO_NEW_MSG:
                case OFFSET_ILLEGAL:
                default:
                    break;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQPullConsumer.shutdown();
            defaultMQAdminExt.shutdown();
        }
    }
}
