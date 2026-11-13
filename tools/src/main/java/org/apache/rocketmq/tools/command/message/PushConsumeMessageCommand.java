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

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

/**
 * Starts a real push consumer from mqadmin so operators can inspect message delivery, progress, and tracing without
 * compiling a standalone demo application.
 */
public class PushConsumeMessageCommand implements SubCommand {
    private final ConsumerFactory consumerFactory;
    private final PrintStream out;

    public PushConsumeMessageCommand() {
        this(DefaultMQPushConsumer::new, System.out);
    }

    PushConsumeMessageCommand(ConsumerFactory consumerFactory, PrintStream out) {
        this.consumerFactory = consumerFactory;
        this.out = out;
    }

    @Override
    public String commandName() {
        return "consumeMessageByPush";
    }

    @Override
    public String commandDesc() {
        return "Consume and inspect messages with a temporary PushConsumer.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option option = new Option("t", "topic", true, "Topic name");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("g", "consumerGroup", true,
            "Consumer group used by the temporary PushConsumer");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("s", "subExpression", true,
            "Subscription expression, for example '*' or 'TagA || TagB'. Default: *");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("c", "messageCount", true,
            "Stop after receiving at least this many messages. 0 means unlimited. Default: 0");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("w", "maxWaitSeconds", true,
            "Stop after this many seconds. 0 means unlimited. Default: 0");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("b", "batchSize", true,
            "Maximum messages delivered to the listener per batch, from 1 to 1024. Default: 1");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("o", "orderly", true,
            "Use orderly consumption. true or false. Default: false");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("m", "messageTraceEnabled", true,
            "Enable message trace. true or false. Default: false");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("r", "customizedTraceTopic", true,
            "Customized trace topic; requires messageTraceEnabled=true");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("d", "printBody", true,
            "Print message body. true or false. Default: true");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("p", "printProperties", true,
            "Print message properties. true or false. Default: false");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("a", "charset", true, "Charset used to decode message bodies. Default: UTF-8");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("i", "instanceName", true,
            "Client instance name. Default: a unique mqadmin_push timestamp");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("f", "consumeFromWhere", true,
            "Initial position for a new group: LAST, FIRST, or TIMESTAMP. Default: LAST");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("x", "consumeTimestamp", true,
            "Start timestamp in yyyyMMddHHmmss format when consumeFromWhere=TIMESTAMP");
        option.setRequired(false);
        options.addOption(option);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        final PushConsumeMessageConfig config;
        try {
            config = PushConsumeMessageConfig.fromCommandLine(commandLine);
        } catch (IllegalArgumentException e) {
            throw new SubCommandException(commandName() + " command has invalid arguments: " + e.getMessage(), e);
        }

        DefaultMQPushConsumer consumer = null;
        PushConsumeMessagePrinter printer = new PushConsumeMessagePrinter(config, out);
        PushConsumeMessageObserver observer = new PushConsumeMessageObserver(config, printer);
        PushConsumeMessageObserver.CompletionReason completionReason = PushConsumeMessageObserver.CompletionReason.STOPPED;
        try {
            consumer = createConsumer(config, rpcHook);
            configureConsumer(consumer, config, observer);
            consumer.start();
            printer.printStarted();
            completionReason = observer.awaitCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            completionReason = PushConsumeMessageObserver.CompletionReason.INTERRUPTED;
        } catch (Exception e) {
            throw new SubCommandException(commandName() + " command failed", e);
        } finally {
            if (consumer != null) {
                consumer.shutdown();
            }
            printer.printSummary(observer.snapshot(), completionReason);
        }
    }

    private DefaultMQPushConsumer createConsumer(PushConsumeMessageConfig config, RPCHook rpcHook) {
        return consumerFactory.create(config.getConsumerGroup(), rpcHook, new AllocateMessageQueueAveragely(),
            config.isMessageTraceEnabled(), config.getCustomizedTraceTopic());
    }

    private void configureConsumer(DefaultMQPushConsumer consumer, PushConsumeMessageConfig config,
        PushConsumeMessageObserver observer) throws Exception {
        consumer.setInstanceName(config.getInstanceName());
        consumer.setConsumeMessageBatchMaxSize(config.getBatchSize());
        consumer.setConsumeFromWhere(config.getConsumeFromWhere());
        if (config.getConsumeTimestamp() != null) {
            consumer.setConsumeTimestamp(config.getConsumeTimestamp());
        }
        consumer.subscribe(config.getTopic(), config.getSubExpression());
        if (config.isOrderly()) {
            consumer.registerMessageListener((org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly) observer);
        } else {
            consumer.registerMessageListener((org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently) observer);
        }
    }

    @FunctionalInterface
    interface ConsumerFactory {
        DefaultMQPushConsumer create(String consumerGroup, RPCHook rpcHook,
            org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            boolean enableMessageTrace, String customizedTraceTopic);
    }
}
