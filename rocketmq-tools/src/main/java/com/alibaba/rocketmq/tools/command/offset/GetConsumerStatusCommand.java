/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.tools.command.offset;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import java.util.Map;


/**
 * @author manhong.yqd
 *
 */
public class GetConsumerStatusCommand implements SubCommand {
    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        GetConsumerStatusCommand cmd = new GetConsumerStatusCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-t qatest_TopicTest", "-g qatest_consumer_broadcast"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                        cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }

    @Override
    public String commandName() {
        return "getConsumerStatus";
    }

    @Override
    public String commandDesc() {
        return "get consumer status from client.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "originClientId", true, "set the consumer clientId");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            String originClientId = "";
            if (commandLine.hasOption("i")) {
                originClientId = commandLine.getOptionValue("i").trim();
            }
            defaultMQAdminExt.start();

            Map<String, Map<MessageQueue, Long>> consumerStatusTable =
                    defaultMQAdminExt.getConsumeStatus(topic, group, originClientId);
            System.out.printf("get consumer status from client. group=%s, topic=%s, originClientId=%s%n",
                    group, topic, originClientId);

            System.out.printf("%-50s  %-15s  %-15s  %-20s%n",//
                    "#clientId",//
                    "#brokerName", //
                    "#queueId",//
                    "#offset");

            for(Map.Entry<String, Map<MessageQueue, Long>> entry: consumerStatusTable.entrySet()){
                String clientId = entry.getKey();
                Map<MessageQueue, Long> mqTable = entry.getValue();
                for(Map.Entry<MessageQueue,Long> entry1: mqTable.entrySet()){
                    MessageQueue mq = entry1.getKey();
                    System.out.printf("%-50s  %-15s  %-15d  %-20d%n",//
                            UtilAll.frontStringAtLeast(clientId, 50),//
                            mq.getBrokerName(),//
                            mq.getQueueId(),//
                            mqTable.get(mq));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
