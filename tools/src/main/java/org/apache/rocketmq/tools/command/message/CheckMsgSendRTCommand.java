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
package org.apache.rocketmq.tools.command.message;

import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class CheckMsgSendRTCommand implements SubCommand {
    private static String brokerName = "";
    private static int queueId = 0;
    private DefaultMQProducer producer;
    private DefaultMQAdminExt adminExt;

    @Override
    public String commandName() {
        return "checkMsgSendRT";
    }

    @Override
    public String commandDesc() {
        return "check message send response time";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("a", "amount", true, "message amount | default 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "size", true, "message size | default 128 Byte");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        if (producer == null) {
            producer = new DefaultMQProducer(rpcHook);
        }
        producer.setProducerGroup(Long.toString(System.currentTimeMillis()));
        if (adminExt == null) {
            adminExt = new DefaultMQAdminExt(rpcHook);
        }

        try {
            adminExt.start();
            producer.start();
            boolean sendSuccess = false;
            String topic = commandLine.getOptionValue('t').trim();
            long amount = !commandLine.hasOption('a') ? 100 : Long.parseLong(commandLine
                .getOptionValue('a').trim());
            long msgSize = !commandLine.hasOption('s') ? 128 : Long.parseLong(commandLine
                .getOptionValue('s').trim());
            Message msg = new Message(topic, getStringBySize(msgSize).getBytes(MixAll.DEFAULT_CHARSET));
            //connect to broker
            TopicRouteData topicRouteData = adminExt.examineTopicRouteInfo(topic);
            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                producer.send(new Message(brokerData.getBrokerName(), getStringBySize(msgSize).getBytes(MixAll.DEFAULT_CHARSET)));
            }

            System.out.printf("%-32s  %-4s  %-20s    %s%n",
                "#Broker Name",
                "#QID",
                "#Send Result",
                "#RT"
            );
            long elapse = 0;
            for (int i = 0; i < amount; i++) {
                long s = System.nanoTime();
                try {
                    producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            int queueIndex = (Integer) arg % mqs.size();
                            MessageQueue queue = mqs.get(queueIndex);
                            brokerName = queue.getBrokerName();
                            queueId = queue.getQueueId();
                            return queue;
                        }
                    }, i);
                    sendSuccess = true;
                } catch (Exception e) {
                    sendSuccess = false;
                }
                long e = System.nanoTime();
                elapse += e - s;
                System.out.printf("%-32s  %-4s  %-20s    %s%n",
                    brokerName,
                    queueId,
                    sendSuccess,
                    (double) (e - s) / 1000000
                );
            }
            double rt = (double) elapse / amount / 1000000;
            System.out.printf("Avg RT: %s%n", String.format("%.2f", rt));
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            producer.shutdown();
            adminExt.shutdown();
        }
    }

    public String getStringBySize(long size) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < size; i++) {
            res.append('a');
        }
        return res.toString();
    }
}
