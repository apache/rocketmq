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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.nio.charset.StandardCharsets;

public class SendMessageCommand implements SubCommand {

    private DefaultMQProducer producer;

    @Override
    public String commandName() {
        return "sendMessage";
    }

    @Override
    public String commandDesc() {
        return "Send a message.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "Topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("p", "body", true, "UTF-8 string format of the message body");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "key", true, "Message keys");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "tags", true, "Message tags");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "Target broker name (requires -i)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "qid", true, "Target queue ID, a non-negative integer (requires -b)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "msgTraceEnable", true, "Message Trace Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private DefaultMQProducer createProducer(RPCHook rpcHook, boolean msgTraceEnable) {
        if (this.producer != null) {
            return producer;
        } else {
            producer = new DefaultMQProducer(null, rpcHook, msgTraceEnable, null);
            producer.setProducerGroup(Long.toString(System.currentTimeMillis()));
            return producer;
        }
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        String topic = commandLine.getOptionValue('t').trim();
        String brokerName = commandLine.getOptionValue('b');
        String queueIdValue = commandLine.getOptionValue('i');
        if ((brokerName == null) != (queueIdValue == null)) {
            throw new SubCommandException("Options -b (--broker) and -i (--qid) must be specified together.");
        }

        MessageQueue targetQueue = null;
        if (brokerName != null) {
            brokerName = brokerName.trim();
            if (brokerName.isEmpty()) {
                throw new SubCommandException("Option -b (--broker) must specify a broker name.");
            }
            int queueId;
            try {
                queueId = Integer.parseInt(queueIdValue.trim());
            } catch (NumberFormatException e) {
                throw new SubCommandException("Option -i (--qid) must be a non-negative integer.", e);
            }
            if (queueId < 0) {
                throw new SubCommandException("Option -i (--qid) must be a non-negative integer.");
            }
            targetQueue = new MessageQueue(topic, brokerName, queueId);
        }

        Message msg;
        String body = commandLine.getOptionValue('p');
        String tag = null;
        String keys = null;
        try {
            if (commandLine.hasOption('k')) {
                keys = commandLine.getOptionValue('k').trim();
            }
            if (commandLine.hasOption('c')) {
                tag = commandLine.getOptionValue('c').trim();
            }
            msg = new Message(topic, tag, keys, body.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        }
        boolean msgTraceEnable = false;
        if (commandLine.hasOption('m')) {
            msgTraceEnable = Boolean.parseBoolean(commandLine.getOptionValue('m').trim());
        }
        DefaultMQProducer producer = this.createProducer(rpcHook, msgTraceEnable);
        SendResult result;
        try {
            producer.start();
            if (targetQueue != null) {
                result = producer.send(msg, targetQueue);
            } else {
                result = producer.send(msg);
            }

        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            producer.shutdown();
        }

        System.out.printf("%-32s  %-4s  %-20s    %s%n",
            "#Broker Name",
            "#QID",
            "#Send Result",
            "#MsgId"
        );

        if (result != null) {
            System.out.printf("%-32s  %-4s  %-20s    %s%n",
                result.getMessageQueue().getBrokerName(),
                result.getMessageQueue().getQueueId(),
                result.getSendStatus(),
                result.getMsgId()
            );
        } else {
            System.out.printf("%-32s  %-4s  %-20s    %s%n",
                "Unknown",
                "Unknown",
                "Failed",
                "None"
            );
        }
    }
}
