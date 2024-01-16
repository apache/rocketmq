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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class AllocateMQSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "allocateMQ";
    }

    @Override
    public String commandDesc() {
        return "Allocate MQ.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "ipList", true, "ipList");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            adminExt.start();

            String topic = commandLine.getOptionValue('t').trim();
            String ips = commandLine.getOptionValue('i').trim();
            final String[] split = ips.split(",");
            final List<String> ipList = new LinkedList<>();
            for (String ip : split) {
                ipList.add(ip);
            }

            final TopicRouteData topicRouteData = adminExt.examineTopicRouteInfo(topic);
            final Set<MessageQueue> mqs = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);

            final AllocateMessageQueueAveragely averagely = new AllocateMessageQueueAveragely();

            RebalanceResult rr = new RebalanceResult();

            for (String i : ipList) {
                final List<MessageQueue> mqResult = averagely.allocate("aa", i, new ArrayList<>(mqs), ipList);
                rr.getResult().put(i, mqResult);
            }

            final String json = RemotingSerializable.toJson(rr, false);
            System.out.printf("%s%n", json);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
