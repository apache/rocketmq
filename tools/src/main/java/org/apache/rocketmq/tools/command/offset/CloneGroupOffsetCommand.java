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

package org.apache.rocketmq.tools.command.offset;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;

public class CloneGroupOffsetCommand implements SubCommand {
    @Override
    public String commandName() {
        return "cloneGroupOffset";
    }

    @Override
    public String commandDesc() {
        return "clone offset from other group.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("s", "srcGroup", true, "set source consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("d", "destGroup", true, "set destination consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "offline", true, "the group or the topic is offline");
        opt.setRequired(false);
        options.addOption(opt);

        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        String srcGroup = commandLine.getOptionValue("s").trim();
        String destGroup = commandLine.getOptionValue("d").trim();
        String topic = commandLine.getOptionValue("t").trim();

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName("admin-" + Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(srcGroup);
            Set<MessageQueue> mqs = consumeStats.getOffsetTable().keySet();
            if (!mqs.isEmpty()) {
                TopicRouteData topicRoute = defaultMQAdminExt.examineTopicRouteInfo(topic);
                for (MessageQueue mq : mqs) {
                    String addr = null;
                    for (BrokerData brokerData : topicRoute.getBrokerDatas()) {
                        if (brokerData.getBrokerName().equals(mq.getBrokerName())) {
                            addr = brokerData.selectBrokerAddr();
                            break;
                        }
                    }
                    long offset = consumeStats.getOffsetTable().get(mq).getBrokerOffset();
                    if (offset >= 0) {
                        defaultMQAdminExt.updateConsumeOffset(addr, destGroup, mq, offset);
                    }
                }
            }
            System.out.printf("clone group offset success. srcGroup[%s], destGroup=[%s], topic[%s]",
                srcGroup, destGroup, topic);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
