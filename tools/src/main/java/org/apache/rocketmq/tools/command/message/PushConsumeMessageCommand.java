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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class PushConsumeMessageCommand implements SubCommand {

    @Override
    public String commandName() {
        return "pushConsumeMessage";
    }

    @Override
    public String commandDesc() {
        return "Consume message.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "Topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "Consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("e", "expression", true, "filter expression");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        try {
            final String group = commandLine.getOptionValue('g').trim();
            final String topic = commandLine.getOptionValue('t').trim();

            final String expression = commandLine.hasOption('e') ? commandLine.getOptionValue('e').trim() : "*";

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group, rpcHook, new AllocateMessageQueueAveragely(), true, null);
            consumer.setInstanceName(Long.toString(System.currentTimeMillis()));

            if (commandLine.hasOption('n')) {
                consumer.setNamesrvAddr(commandLine.getOptionValue('n').trim());
            }

            consumer.subscribe(topic, MessageSelector.byTag(expression));
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                PrintMessageByQueueCommand.printMessage(msgs, "UTF-8",
                    true, true);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (consumer != null) {
                    consumer.shutdown();
                }
                System.out.print("Push consumer shutdown OK.\n");
            }));
            System.out.print("GroupId=" + group + ", topic=" + topic + ", expression=" + expression + ", Push consumer start OK.\n");
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }

}